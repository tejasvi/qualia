from __future__ import annotations

from base64 import urlsafe_b64encode, urlsafe_b64decode
from json import loads, dumps
from logging import getLogger
from secrets import token_bytes
from subprocess import run
from threading import Lock
from time import time_ns
from typing import Union, cast, Optional
from uuid import UUID

import lmdb
from lmdb import Cursor, Environment

from qualia.config import _DB_FOLDER, _GIT_FOLDER
from qualia.models import NodeId, JSONType, Cursors


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
    _env: Optional[Environment] = None
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


def conflict(new_lines: list[str], old_lines: list[str]) -> list[str]:
    return ["<<<<<<< OLD"] + old_lines + ["======="] + new_lines + [
        ">>>>>>> NEW"] if new_lines != old_lines else new_lines


def get_key_val(key: Union[str, bytes], cursor: Cursor) -> JSONType:
    value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
    return None if value_bytes is None else loads(value_bytes.decode())


def put_key_val(key: Union[str, bytes], val: JSONType, cursor: Cursor, overwrite) -> None:
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
    result = run(["git"] + arguments, check=True, cwd=_GIT_FOLDER, capture_output=True)
    stdout = '\n'.join([stream.decode() for stream in (result.stdout, result.stderr)]).strip()
    logger.debug(f"Git:\n{stdout}\n")
    return stdout


logger = getLogger("qualia")
