from __future__ import annotations

from base64 import urlsafe_b64encode
from bisect import bisect_left, insort
from hashlib import sha256
from json import loads, dumps
from logging import getLogger
from os import PathLike
from re import split
from secrets import token_bytes, token_urlsafe
from subprocess import run, CalledProcessError
from threading import Lock, Thread
from time import time_ns
from traceback import format_exception
from typing import Union, cast, Optional, Iterable, Callable, Dict, IO, Iterator, TYPE_CHECKING
from uuid import UUID, uuid4

import lmdb
from lmdb import Cursor, Environment
from orderedset import OrderedSet

from qualia.config import _DB_FOLDER, _GIT_FOLDER, _LOGGER_NAME, _CLIENT_KEY, _TRANSPOSED_FILE_PREFIX, \
    _CONFLICT_MARKER, ENCRYPT_DB, _ENCRYPTION_KEY_FILE, _ENCRYPTION_USED
from qualia.models import NodeId, JSONType, Cursors, View, DbClient, CustomCalledProcessError, KeyNotFoundError, El, Li
from qualia.services.backup import removesuffix

if TYPE_CHECKING:
    from pynvim import Nvim


def get_time_uuid() -> NodeId:
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    id_bytes = left_padded_time + token_bytes(10)
    return cast(NodeId, str(UUID(bytes=id_bytes)))


class Database:
    """
    For some reason environment cannot be nested therefore . E.g. if nesting in
    set_bloom_filter(), the db is empty on next run.
    Relevant? > Repeat Environment.open_db() calls for the same name will return the same handle.
    """
    _db_names = (
        "content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views", "buffer_id_node_id",
        "node_id_buffer_id", "metadata", "bloom_filters", "parents", "transposed_views")
    _env: Environment = None
    _env_open_lock = Lock()

    def __init__(self) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        if Database._env is None:  # Reduce lock contention (rarely an issue)
            with Database._env_open_lock:  # Thread critical section
                if Database._env is None:
                    Database._env = lmdb.open(_DB_FOLDER.as_posix(), max_dbs=len(Database._db_names), map_size=2 ** 20)

    def __enter__(self) -> Cursors:
        self.txn = self._env.begin(write=True)
        cursors = Cursors(**{db_name: self.sub_db(db_name) for db_name in Database._db_names})
        return cursors

    def sub_db(self, db_name: str) -> Cursor:
        return self.txn.cursor(self._env.open_db(db_name.encode(), self.txn))

    def __exit__(self, *args) -> None:
        self.txn.__exit__(*args)


def conflict(new_lines: Li, old_lines: Li) -> Li:
    if new_lines == old_lines:
        return new_lines
    else:
        # Prevent oscillating sync conflicts with _OrderedSet_esque merge (like children conflicts)
        conflicting_content_lines: list[Li] = []
        for content_lines in _splitlines_conflict_marker(old_lines) + _splitlines_conflict_marker(new_lines):
            content_lines.append(_CONFLICT_MARKER)
            idx = bisect_left(conflicting_content_lines, content_lines)
            if idx == len(conflicting_content_lines) or conflicting_content_lines[idx] != content_lines:
                insort(conflicting_content_lines, content_lines)

        merged_content_lines = cast(Li, [content_line for content_lines in conflicting_content_lines for content_line in
                                         content_lines])
        merged_content_lines.pop()  # Trailing _CONFLICT_MARKER
        return merged_content_lines


def _splitlines_conflict_marker(new_lines: Li) -> list[Li]:
    splitted_lines_list = []
    last_conflict_idx = 0
    for idx, line in enumerate(new_lines):
        if line == _CONFLICT_MARKER:
            splitted_lines_list.append(new_lines[last_conflict_idx: idx])
            last_conflict_idx = idx + 1
    splitted_lines_list.append(new_lines[last_conflict_idx:])
    return cast(list[Li], splitted_lines_list)


def get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool) -> JSONType:
    value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
    if must_exist and value_bytes is None:
        raise KeyNotFoundError(key)
    return None if value_bytes is None else loads(value_bytes.decode())


def set_key_val(key: Union[str, bytes], val: JSONType, cursor: Cursor, overwrite: bool) -> None:
    cursor.put(key if isinstance(key, bytes) else key.encode(), dumps(val).encode(), overwrite=overwrite)


def file_name_to_node_id(full_name: str, extension: str) -> NodeId:
    if full_name.endswith(extension):
        node_id = removesuffix(full_name.lstrip(_TRANSPOSED_FILE_PREFIX), extension)
        UUID(node_id)
        return cast(NodeId, node_id)
    else:
        raise ValueError


# @line_profiler_pycharm.profile


def cd_run_git_cmd(arguments: list[str]) -> str:
    try:
        result = run(["git"] + arguments, check=True, cwd=_GIT_FOLDER, capture_output=True, text=True)
    except CalledProcessError as e:
        raise CustomCalledProcessError(e)
    stdout = f"{result.stdout}{result.stderr}".strip()
    logger.debug(f"Git:\n{stdout}\n")
    return stdout


logger = getLogger(_LOGGER_NAME)


def exception_traceback(e: BaseException) -> str:
    return '\n' + '\n'.join(format_exception(None, e, e.__traceback__))


def get_node_descendants(cursors: Cursors, node_id: NodeId, transposed: bool, discard_invalid: bool) -> OrderedSet[
    NodeId]:
    node_descendants = cast(OrderedSet[NodeId], OrderedSet(
        get_key_val(node_id, cursors.parents if transposed else cursors.children, False) or []))
    if not discard_invalid:
        return node_descendants
    to_remove = set()
    for descendant_id in node_descendants:
        if not cursors.content.set_key(descendant_id.encode()):
            to_remove.add(descendant_id)
    if to_remove:
        for descendant_id in to_remove:
            delete_node(cursors, descendant_id)
        node_descendants.difference_update(to_remove)
        _set_node_descendants_value(node_descendants, cursors, node_id, transposed)
    return node_descendants


def delete_node(cursors: Cursors, node_id: NodeId) -> None:
    for cursor in (cursors.children, cursors.content, cursors.views,
                   cursors.unsynced_children, cursors.unsynced_content, cursors.unsynced_views,
                   cursors.parents, cursors.transposed_views, cursors.node_id_buffer_id,
                   cursors.bloom_filters):
        if cursor.set_key(node_id.encode()):
            cursor.delete()


def _set_node_descendants_value(descendant_ids: OrderedSet[NodeId], cursors: Cursors, node_id: NodeId,
                                transposed: bool) -> None:
    set_key_val(node_id, list(descendant_ids), cursors.parents if transposed else cursors.children, True)
    if not transposed:
        set_key_val(node_id, True, cursors.unsynced_children, True)


def get_db_node_content_lines(cursors: Cursors, node_id: NodeId) -> Union[El, Li]:
    db_value = cast(El, get_key_val(node_id, cursors.content, True))
    return db_value


def get_node_content_lines(cursors: Cursors, node_id: NodeId) -> Li:
    db_node_content_lines = get_db_node_content_lines(cursors, node_id)
    db_value = decrypt_lines(cast(El, db_node_content_lines)) if ENCRYPT_DB else cast(Li, db_node_content_lines)
    return db_value


def set_node_content_lines(node_id: NodeId, content_lines: Li, cursors: Cursors) -> None:
    set_key_val(node_id, encrypt_lines(content_lines) if ENCRYPT_DB else content_lines, cursors.content, True)
    set_key_val(node_id, True, cursors.unsynced_content, True)
    if cursors.bloom_filters.set_key(node_id.encode()):
        cursors.bloom_filters.delete()


def ordered_data_hash(data: Union[Li, list[NodeId]]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def children_data_hash(data: Iterable[NodeId]) -> str:
    return ordered_data_hash(sorted(data))


def normalized_search_prefixes(string: str) -> set[str]:
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}


def save_root_view(view: View, views_cur: Cursor) -> None:
    set_key_val(view.main_id, cast(Optional[dict[str, object]], view.sub_tree), views_cur, True)


def _add_remove_ancestor(add_or_remove: bool, ancestor_id: NodeId, descendant_ids: Iterable[NodeId], cursors: Cursors,
                         transposed: bool):
    for descendant_id in descendant_ids:
        ancestor_id_list = get_node_descendants(cursors, descendant_id, not transposed, False)
        if add_or_remove:
            ancestor_id_list.add(ancestor_id)
        else:
            ancestor_id_list.remove(ancestor_id)
        _set_node_descendants_value(ancestor_id_list, cursors, descendant_id, not transposed)


def set_node_descendants(node_id: NodeId, descendant_ids: OrderedSet[NodeId], cursors: Cursors, transposed: bool):
    # Order important. (get then set)
    previous_node_descendants = get_node_descendants(cursors, node_id, transposed, False)

    _add_remove_ancestor(True, node_id, descendant_ids.difference(previous_node_descendants), cursors, transposed)
    _add_remove_ancestor(False, node_id, previous_node_descendants.difference(descendant_ids), cursors, transposed)

    _set_node_descendants_value(descendant_ids, cursors, node_id, transposed)


class StartLoggedThread(Thread):
    def __init__(self, target: Callable, name: str):
        def logged_target() -> None:
            try:
                target()
            except BaseException as e:
                logger.critical("Exception in thread " + name + "\n" + exception_traceback(e))
                raise e

        super().__init__(target=logged_target, name=name)
        self.start()


def get_set_client(metadata_cursor: Cursor) -> DbClient:
    db_client_data = cast(dict, get_key_val(_CLIENT_KEY, metadata_cursor, False))
    if db_client_data is None:
        client_details = DbClient(client_id=str(_get_uuid()), client_name=f"nvim:{token_urlsafe(1)}")
        set_key_val(_CLIENT_KEY, cast(Dict, client_details), metadata_cursor, False)
    else:
        client_details = DbClient(client_id=db_client_data["client_id"], client_name=db_client_data["client_name"])
    return client_details


def _get_uuid() -> NodeId:
    return cast(NodeId, str(uuid4()))


def open_write_lf(file_path: Union[str, bytes, PathLike], prevent_overwrite: bool) -> IO:
    return open(file_path, 'x' if prevent_overwrite else 'w', newline='\n')


def cursor_keys(cursor: Cursor) -> Iterator[str]:
    cursor.first()
    for key_bytes in cursor.iternext(values=False):
        yield key_bytes.decode()


if _ENCRYPTION_USED:
    from cryptography.fernet import Fernet

    fernet = Fernet(_ENCRYPTION_KEY_FILE.read_bytes())
else:
    from qualia.models import AbstractFernet as AbstractFernet

    fernet = AbstractFernet(b"dummy_key")  # noqa[assignment]


def decrypt_lines(encrypted_lines: El) -> Li:
    return cast(Li, fernet.decrypt(encrypted_lines[0].encode()).decode().split('\n'))


def encrypt_lines(unencrypted_lines: Li) -> El:
    return cast(El, [fernet.encrypt('\n'.join(unencrypted_lines).encode()).decode()])


def children_hash(key: NodeId, cursors: Cursors) -> str:
    return children_data_hash(get_node_descendants(cursors, key, False, True))


def trigger_buffer_change(nvim):
    # type:(Nvim) -> None
    nvim.async_call(nvim.command,
                    'execute (expand("%:p")[-5:] ==? ".q.md" && mode() !=# "t") ? "normal VyVp" : ""',
                    async_=True)


absent_node_content_lines = cast(Li, [''])
