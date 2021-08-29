from __future__ import annotations

from base64 import urlsafe_b64encode, urlsafe_b64decode
from collections import defaultdict
from difflib import Differ
from hashlib import sha256
from json import loads, dumps
from logging import getLogger
from re import split
from secrets import token_bytes, token_urlsafe
from subprocess import run, CalledProcessError
from threading import Lock, Thread
from time import time_ns
from traceback import format_exception
from typing import Union, cast, Optional, Iterable, Callable, Any, Dict
from uuid import UUID, uuid4

import lmdb
from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32
from lmdb import Cursor, Environment
from orderedset import OrderedSet

from qualia.config import _DB_FOLDER, _GIT_FOLDER, _LOGGER_NAME, _CLIENT_KEY
from qualia.models import NodeId, JSONType, Cursors, View, ProcessState, LastSeen, RealtimeData, RealtimeContentData, \
    Client, CustomCalledProcessError


def get_time_uuid() -> NodeId:
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    id_bytes = left_padded_time + token_bytes(10)
    return cast(NodeId, urlsafe_b64encode(id_bytes).decode())


class Database:
    """
    For some reason environment cannot be nested therefore . E.g. if nesting in
    save_bloom_filter(), the db is empty on next run.
    Relevant? > Repeat Environment.open_db() calls for the same name will return the same handle.
    """
    _db_names = (
        "content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views", "buffer_to_node_id",
        "node_to_buffer_id", "metadata", "bloom_filters", "parents", "transposed_views")
    _env: Environment = None
    _env_open_lock = Lock()

    def __init__(self) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        if Database._env is None:  # Reduce lock contention
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


differ_compare = Differ().compare


def conflict(new_lines: list[str], old_lines: list[str]) -> list[str]:
    if new_lines == old_lines:
        return new_lines
    else:
        # Differ requires newlines at the end
        return [line[:-1] for line in differ_compare(*[[line+'\n' for line in lines] for lines in (old_lines, new_lines)])]


def get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool) -> JSONType:
    value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
    assert not (must_exist and value_bytes is None)
    return None if value_bytes is None else loads(value_bytes.decode())


def put_key_val(key: Union[str, bytes], val: JSONType, cursor: Cursor, overwrite: bool) -> None:
    cursor.put(key if isinstance(key, bytes) else key.encode(), dumps(val).encode(), overwrite=overwrite)


def removesuffix(input_string: str, suffix: str) -> str:
    # pre 3.9 str.removesuffix
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


def removeprefix(input_string: str, suffix: str) -> str:
    # pre 3.9 str.removeprefix
    if suffix and input_string.startswith(suffix):
        return input_string[len(suffix):]
    return input_string


def file_name_to_node_id(name: str, remove_suffix: str) -> NodeId:
    if name.endswith(remove_suffix):
        node_id_hex = removesuffix(name, remove_suffix)
    else:
        raise ValueError
    node_id = NodeId(urlsafe_b64encode(UUID(node_id_hex).bytes).decode())
    return node_id


# @line_profiler_pycharm.profile


def node_id_to_hex(node_id: NodeId) -> str:
    return str(UUID(bytes=urlsafe_b64decode(node_id)))


def cd_run_git_cmd(arguments: list[str]) -> str:
    try:
        result = run(["git"] + arguments, check=True, cwd=_GIT_FOLDER, capture_output=True)
    except CalledProcessError as e:
        raise CustomCalledProcessError(e)
    stdout = '\n'.join([stream.decode() for stream in (result.stdout, result.stderr)]).strip()
    logger.debug(f"Git:\n{stdout}\n")
    return stdout


logger = getLogger(_LOGGER_NAME)


def exception_traceback(e: Exception) -> str:
    return '\n'.join(format_exception(None, e, e.__traceback__))


def get_node_descendants(cursors: Cursors, node_id: NodeId, transposed: bool) -> OrderedSet[NodeId]:
    return OrderedSet(get_key_val(node_id, cursors.parents if transposed else cursors.children, False) or [])


def set_node_descendants(descendant_ids: OrderedSet[NodeId], cursors: Cursors, node_id: NodeId, transposed: bool) -> None:
    put_key_val(node_id, list(descendant_ids), cursors.parents if transposed else cursors.children, True)
    if not transposed:
        put_key_val(node_id, True, cursors.unsynced_children, True)


def get_node_content(cursors: Cursors, node_id: NodeId) -> list[str]:
    return cast(list[str], get_key_val(node_id, cursors.content, True))


def set_node_content_lines(content_lines: list[str], cursors: Cursors, node_id: NodeId) -> None:
    put_key_val(node_id, content_lines, cursors.content, True)
    put_key_val(node_id, True, cursors.unsynced_content, True)
    save_bloom_filter(node_id, content_lines, cursors.bloom_filters)


def save_bloom_filter(node_id: NodeId, content_lines: list[str], bloom_filters_cursor: Cursor):
    bloom_filter = BloomFilter(expected_insertions=100, err_rate=0.1, strategy=MURMUR128_MITZ_32)
    string = '\n'.join(content_lines)
    prefixes = normalized_prefixes(string)
    for prefix in prefixes:
        bloom_filter.put(prefix)
    bloom_filters_cursor.put(node_id.encode(), bloom_filter.dumps())


def sync_with_db(root_view: Optional[View], changes: ProcessState, last_seen: LastSeen, cursors: Cursors,
                 transposed: bool, realtime: bool) -> RealtimeData:
    if root_view:
        save_root_view(root_view, cursors.transposed_views if transposed else cursors.views)
        if not transposed:
            put_key_val(root_view.main_id, True, cursors.unsynced_views, True)

    realtime_content_data = sync_content(changes.changed_content_map, cursors, last_seen, realtime)
    realtime_descendants_data = sync_descendants(changes.changed_descendants_map, cursors, last_seen, transposed, realtime)
    realtime_children_data = {node_id: (realtime_data_hash(children_ids), children_ids) for node_id, children_ids in (
        transpose_dict(realtime_descendants_data) if transposed else realtime_descendants_data).items()}
    return {"content": realtime_content_data, "children": realtime_children_data} if realtime else {}


def sync_descendants(changed_descendants_map: dict[NodeId, OrderedSet[NodeId]], cursors: Cursors,
                     last_seen: LastSeen, transposed: bool, realtime: bool) -> dict[NodeId, list[NodeId]]:
    realtime_descendants_data = {}
    for node_id, descendants_ids in changed_descendants_map.items():
        db_descendants_ids = get_node_descendants(cursors, node_id, transposed)
        if node_id not in last_seen or (db_descendants_ids != last_seen[node_id].descendants_ids):
            descendants_ids.update(db_descendants_ids)

        set_ancestor_descendants(cursors, descendants_ids, node_id, transposed)

        if realtime:
            realtime_descendants_data[node_id] = list(descendants_ids)
    return realtime_descendants_data


def set_ancestor(cursors: Cursors, old_descendants_ids: OrderedSet[NodeId], new_descendants_ids: OrderedSet[NodeId],
                 ancestor_id: NodeId, transposed: bool) -> None:
    _add_remove_ancestor(True, ancestor_id, new_descendants_ids.difference(old_descendants_ids), cursors, transposed)
    _add_remove_ancestor(False, ancestor_id, old_descendants_ids.difference(new_descendants_ids), cursors, transposed)


def sync_content(changed_content_map: dict[NodeId, list[str]], cursors: Cursors, last_seen: LastSeen,
                 realtime) -> RealtimeContentData:
    realtime_content_data: RealtimeContentData = {}
    for node_id, content_lines in changed_content_map.items():
        db_content_lines = cast(Optional[list], get_key_val(node_id, cursors.content, False))
        if db_content_lines is not None:
            if node_id not in last_seen or db_content_lines != last_seen[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines)
        set_node_content_lines(content_lines, cursors, node_id)

        if realtime:
            realtime_content_data[node_id] = realtime_data_hash(db_content_lines), content_lines

    return realtime_content_data


def realtime_data_hash(data: Union[bytes, JSONType]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def normalized_prefixes(string: str) -> set[str]:
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}


def save_root_view(view: View, views_cur: Cursor) -> None:
    put_key_val(view.main_id, cast(Optional[dict[str, Any]], view.sub_tree), views_cur, True)


def transpose_dict(dictionary: dict[NodeId, list[NodeId]]) -> dict[NodeId, list[NodeId]]:
    transposed = defaultdict(list)
    for node_id, descendants in dictionary.items():
        for descendant_id in descendants:
            transposed[descendant_id].append(node_id)
    transposed.default_factory = None
    return transposed


def _add_remove_ancestor(add_or_remove: bool, ancestor_id: NodeId, descendant_ids: Iterable[NodeId], cursors: Cursors,
                         transposed: bool):
    for descendant_id in descendant_ids:
        ancestor_cursor = cursors.children if transposed else cursors.parents
        ancestor_id_list = OrderedSet(
            get_key_val(descendant_id, ancestor_cursor, False) or [])
        if add_or_remove:
            ancestor_id_list.add(ancestor_id)
        else:
            ancestor_id_list.remove(ancestor_id)
        set_node_descendants(ancestor_id_list, cursors, descendant_id, not transposed)


def set_ancestor_descendants(cursors: Cursors, descendant_ids: OrderedSet[NodeId], node_id: NodeId, transposed: bool):
    # Order important. (get then set)
    set_ancestor(cursors, get_node_descendants(cursors, node_id, transposed), descendant_ids, node_id, transposed)
    set_node_descendants(descendant_ids, cursors, node_id, transposed)


class StartLoggedThread(Thread):
    def __init__(self, target: Callable, name: str):
        def logged_target() -> None:
            try:
                target()
            except Exception as e:
                logger.critical("Exception in thread " + name + "\n" + exception_traceback(e))

        super().__init__(target=logged_target, name=name)
        self.start()


def get_set_client(metadata_cursor: Cursor) -> Client:
    db_client_data = cast(dict, get_key_val(_CLIENT_KEY, metadata_cursor, False))
    if db_client_data is None:
        client_details = Client(client_id=str(get_uuid()), client_name=f"Vim-{token_urlsafe(1)}")
        put_key_val(_CLIENT_KEY, cast(Dict, client_details), metadata_cursor, False)
    else:
        client_details = Client(client_id=db_client_data["client_id"], client_name=db_client_data["client_name"])
    return client_details


def get_uuid() -> NodeId:
    return cast(NodeId, urlsafe_b64encode(uuid4().bytes).decode())
