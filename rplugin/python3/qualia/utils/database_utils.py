from json import loads, dumps
from threading import Lock
from typing import Optional, Union, overload, Literal

import lmdb
from lmdb import Cursor, Environment

from qualia.config import _DB_FOLDER
from qualia.models import JSONType, KeyNotFoundError, Cursors, NodeId


class LMDB:
    """
    For some reason environment cannot be nested. E.g. if nesting in set_bloom_filter(), the db is empty on next run.
    Relevant? "Repeat Environment.open_db() calls for the same name will return the same handle."
    """
    _env_open_lock = Lock()
    _env: Environment = None

    def __init__(self) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        if self._env is None:  # Reduce lock contention (rarely an issue)
            with self._env_open_lock:  # Thread critical section
                if self._env is None:
                    self._env = lmdb.open(_DB_FOLDER.as_posix(), max_dbs=len(Cursors.__annotations__), map_size=1e9)

    def __enter__(self):
        # type:(LMDB) -> LMDB
        self._txn = self._env.begin(write=True)
        self._cursors = Cursors(**{db_name: self._sub_db(db_name) for db_name in Cursors.__annotations__})
        return self

    def _sub_db(self, db_name: str) -> Cursor:
        return self._txn.cursor(self._env.open_db(db_name.encode(), self._txn))

    def __exit__(self, *args) -> None:
        self._txn.__exit__(*args)

    def delete_node(self, node_id: NodeId) -> None:
        cursors = self._cursors
        for cursor in (cursors.children, cursors.content, cursors.views, cursors.parents, cursors.transposed_views,
                       cursors.node_id_buffer_id, cursors.bloom_filters):
            if cursor.set_key(node_id.encode()):
                cursor.delete()
        for cursor in (cursors.unsynced_children, cursors.unsynced_content, cursors.unsynced_views):
            self.set_unsynced(cursor, node_id)

    def is_valid_node(self, node_id: NodeId) -> bool:
        return bool(self._get_key_val(node_id, self._cursors.content, False, True))

    @staticmethod
    def set_unsynced(cursor: Cursor, node_id: NodeId) -> None:
        LMDB._set_key_val(node_id, b"", cursor, True)

    @staticmethod
    @overload
    def _get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool, raw_bytes: Literal[False]) -> JSONType:
        ...

    @staticmethod
    @overload
    def _get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool, raw_bytes: Literal[True]) -> Optional[
        bytes]:
        ...

    @staticmethod
    def _get_key_val(key: Union[str, bytes], cursor: Cursor, must_exist: bool, raw_bytes: bool) -> Union[
        JSONType, bytes]:
        value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
        if must_exist and value_bytes is None:
            raise KeyNotFoundError(key)
        return None if value_bytes is None else (value_bytes if raw_bytes else loads(value_bytes.decode()))

    @staticmethod
    def _set_key_val(key: Union[str, bytes], val: Union[JSONType, bytes], cursor: Cursor,
                     ignore_existing: bool) -> None:
        cursor.put(key if isinstance(key, bytes) else key.encode(),
                   val if isinstance(val, bytes) else dumps(val).encode(), overwrite=ignore_existing)

    @staticmethod
    def _cursor_keys(cursor: Cursor) -> list[str]:
        cursor.first()
        return [key_bytes.decode() for key_bytes in cursor.iternext(values=False)]
