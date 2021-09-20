from json import loads, dumps
from threading import Lock
from typing import Union

import lmdb
from lmdb import Cursor, Environment

from qualia.config import _DB_FOLDER
from qualia.models import JSONType, KeyNotFoundError, Cursors, NodeId


def _get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool) -> JSONType:
    value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
    if must_exist and value_bytes is None:
        raise KeyNotFoundError(key)
    return None if value_bytes is None else loads(value_bytes.decode())


def _set_key_val(key: Union[str, bytes], val: JSONType, cursor: Cursor, overwrite: bool) -> None:
    cursor.put(key if isinstance(key, bytes) else key.encode(), dumps(val).encode(), overwrite=overwrite)


def _pop_if_exists(cursor: Cursor, key: str) -> bool:
    if cursor.set_key(key.encode()):
        return cursor.delete()
    return False


def _cursor_keys(cursor: Cursor) -> list[str]:
    cursor.first()
    return [key_bytes.decode() for key_bytes in cursor.iternext(values=False)]


class _LMDB:
    """
    For some reason environment cannot be nested. E.g. if nesting in set_bloom_filter(), the db is empty on next run.
    Relevant? "Repeat Environment.open_db() calls for the same name will return the same handle."
    """
    _env_open_lock = Lock()
    _env: Environment = None
    _db_names = (
        "content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views",
        "buffer_id_bytes_node_id",
        "node_id_buffer_id", "metadata", "bloom_filters", "parents", "transposed_views")

    def __init__(self) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        if self._env is None:  # Reduce lock contention (rarely an issue)
            with self._env_open_lock:  # Thread critical section
                if self._env is None:
                    self._env = lmdb.open(_DB_FOLDER.as_posix(), max_dbs=len(self._db_names), map_size=1e9)

    def __enter__(self):
        # type:(_LMDB) -> _LMDB
        self._txn = self._env.begin(write=True)
        self._cursors = Cursors(**{db_name: self._sub_db(db_name) for db_name in self._db_names})
        return self

    def _sub_db(self, db_name: str) -> Cursor:
        return self._txn.cursor(self._env.open_db(db_name.encode(), self._txn))

    def __exit__(self, *args) -> None:
        self._txn.__exit__(*args)

    def delete_node(self, node_id: NodeId) -> None:
        cursors = self._cursors
        for cursor in (cursors.children, cursors.content, cursors.views,
                       cursors.unsynced_children, cursors.unsynced_content, cursors.unsynced_views,
                       cursors.parents, cursors.transposed_views, cursors.node_id_buffer_id,
                       cursors.bloom_filters):
            if cursor.set_key(node_id.encode()):
                cursor.delete()
