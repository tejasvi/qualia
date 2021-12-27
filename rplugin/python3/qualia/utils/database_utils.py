import os
from json import loads, dumps
from os.path import getsize
from pathlib import Path
from threading import Lock
from typing import Optional, Union, overload, Literal, Type, Callable

import lmdb
from lmdb import Cursor, Environment, MapFullError, Transaction

from qualia.config import DbMetaKey
from qualia.models import JSONType, KeyNotFoundError, Cursors, NodeId, QCursors, CursorT
from qualia.utils.common_utils import acquire_process_lock


class LMDB:
    """
    For some reason environment cannot be nested. E.g. if nesting in set_bloom_filter(), the db is empty on next run.
    Relevant? "Repeat Environment.open_db() calls for the same name will return the same handle."
    """
    _env_open_lock = Lock()
    _envs: dict[Path, Environment] = {}

    def __init__(self) -> None:
        # Must call _set_lmdb_env() to set env later
        self._env: Environment = NotImplemented
        self.last_db_path: Optional[Path] = None
        self._txn: Optional[Transaction] = None
        self._cursors: Optional[Cursors] = None

    def _set_lmdb_env(self, db_path: str) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        path = Path(db_path).absolute()
        self.last_db_path = path
        if path not in LMDB._envs:  # Thread unsafe precheck to skip lock aquires
            with LMDB._env_open_lock:  # Thread critical section
                if path not in LMDB._envs:
                    lock = acquire_process_lock(f"open_db.qualia.lock", path, 10, 1)  # lmdb.open calls env.set_mapsize

                    # db_file_path = path.joinpath('data.mdb')
                    # map_size = getsize(db_file_path) * 2 if db_file_path.exists() else 2 ** 20
                    # if os.name != 'nt':  # Windows' sparse files not supported https://github.com/jnwatson/py-lmdb/issues/85
                    #     map_size = max(map_size, 2**30)
                    map_size = None  # To test self.double_mapsize

                    LMDB._envs[path] = lmdb.open(path.as_posix(), max_dbs=len(Cursors.__annotations__), map_size=map_size)

                    lock.__exit__()
        self._env = LMDB._envs[path]

    def double_mapsize(self) -> None:
        lock = acquire_process_lock(f"open_db.qualia.lock", self.last_db_path, 10, 1)
        self._env.set_mapsize(self._env.info()["map_size"] * 2)
        lock.__exit__()

    def activate_cursors(self, cursor_type: Callable[[str, Cursor], Union[Cursors, QCursors]]):
        self._txn = self._env.begin(write=True)
        self._cursors = cursor_type(**{db_name: self._sub_db(db_name) for db_name in cursor_type.__annotations__})
        return self

    def __enter__(self):
        # type:(LMDB) -> LMDB
        self.activate_cursors(Cursors)
        return self

    def _sub_db(self, db_name: str) -> Cursor:
        return self._txn.cursor(self._env.open_db(db_name.encode(), self._txn))

    def __exit__(self, *args) -> None:
        self._txn.__exit__(*args)

    def delete_node(self, node_id: NodeId) -> None:
        cursors = self._cursors
        for cursor in (cursors.children, cursors.content, cursors.views, cursors.parents, cursors.transposed_views, cursors.bloom_filters):
            LMDB._remove_key(node_id, cursor, True)
        for cursor in (cursors.unsynced_children, cursors.unsynced_content, cursors.unsynced_views):
            self.set_unsynced(cursor, node_id)

    def is_valid_node(self, node_id: NodeId) -> bool:
        return bool(self._get_key_val(node_id, self._cursors.content, False, True))

    @staticmethod
    def set_unsynced(cursor: Cursor, node_id: NodeId) -> None:
        LMDB._set_key_val(node_id, b"", cursor, True)

    @staticmethod
    @overload
    def _get_key_val(key: Union[str, bytes, DbMetaKey], cursor: Cursor, must_exist: bool, raw_bytes: Literal[False]) -> JSONType:
        ...

    @staticmethod
    @overload
    def _get_key_val(key: Union[str, bytes, DbMetaKey], cursor: Cursor, must_exist: bool, raw_bytes: Literal[True]) -> Optional[
        bytes]:
        ...

    @staticmethod
    def _get_key_val(key: Union[str, bytes, DbMetaKey], cursor: Cursor, must_exist: bool, raw_bytes: bool) -> Union[
            JSONType, bytes]:
        value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
        if must_exist and value_bytes is None:
            raise KeyNotFoundError(key)
        return None if value_bytes is None else (value_bytes if raw_bytes else loads(value_bytes.decode()))

    @staticmethod
    def _set_key_val(key: Union[str, bytes, DbMetaKey], val: Union[JSONType, bytes], cursor: Cursor,
                     ignore_existing: bool) -> None:
        cursor.put(key if isinstance(key, bytes) else key.encode(),
                   val if isinstance(val, bytes) else dumps(val).encode(), overwrite=ignore_existing)

    @staticmethod
    def _remove_key(key: Union[str, bytes, DbMetaKey], cursor: Cursor, ignore_existing: bool):
        if cursor.set_key(key if isinstance(key, bytes) else key.encode()):
            cursor.delete()
        elif not ignore_existing:
            raise KeyNotFoundError

    @staticmethod
    def _cursor_keys(cursor: Cursor) -> list[str]:
        cursor.first()
        return [key_bytes.decode() for key_bytes in cursor.iternext(values=False)]


class MLMDB(LMDB):
    _cursors: Optional[QCursors]

    def __enter__(self):
        # type:(MLMDB) -> MLMDB
        self.activate_cursors(QCursors)
        return self
