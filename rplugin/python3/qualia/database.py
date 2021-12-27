from abc import ABC, abstractmethod
from base64 import urlsafe_b64encode
from collections import defaultdict
from itertools import chain
from os import urandom
from typing import Union, cast, Iterable, Optional, Container, overload, Literal, Callable

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives._serialization import PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from lmdb import Cursor
from orderedset import OrderedSet

from qualia.config import ENCRYPT_DB, _SHORT_ID_STORE_BYTES, _DB_FOLDER, DbMetaKey
from qualia.models import NodeId, El, Li, View, Tree, NodeShortId, MinimalDb, SourceId, DbType, KeyNotFoundError, FullId, ShortId, SourceShortId, SyncableDb, MutableDb
from qualia.utils.bootstrap_utils import setup_repository
from qualia.utils.common_utils import get_uuid, children_data_hash, \
    fernet, normalized_search_prefixes, short_id_encoder, StartLoggedThread, get_time_uuid
from qualia.utils.database_utils import LMDB, MLMDB


class _DbNodeIds(MLMDB):
    def short_id_bytes_to_full_id(self, short_id_bytes: bytes) -> FullId:
        return cast(NodeId, LMDB._get_key_val(short_id_bytes, self._cursors.short_id_bytes_node_id, True, False))

    @overload
    def full_to_short_id(self, full_id: NodeId, node_or_source_id: Literal[True]) -> NodeShortId:
        ...

    @overload
    def full_to_short_id(self, full_id: SourceId, node_or_source_id: Literal[False]) -> SourceShortId:
        ...

    def full_to_short_id(self, full_id: FullId, node_or_source_id: bool) -> ShortId:
        cursors = self._cursors
        full_id_short_id_cursor = cursors.node_id_short_id if node_or_source_id else cursors.source_id_short_id
        short_id_bytes_full_id_cursor = cursors.short_id_bytes_node_id if node_or_source_id else cursors.short_id_bytes_source_id

        short_id_bytes = LMDB._get_key_val(full_id, full_id_short_id_cursor, False, True)
        if short_id_bytes is None:
            if short_id_bytes_full_id_cursor.last():
                last_short_id_bytes = short_id_bytes_full_id_cursor.key()
                new_counter = int.from_bytes(last_short_id_bytes, 'big') + 1
                short_id_bytes = new_counter.to_bytes(_SHORT_ID_STORE_BYTES, 'big')
            else:
                short_id_bytes = (0).to_bytes(_SHORT_ID_STORE_BYTES, 'big')
            # base65536 doesn't output brackets https://qntm.org/safe
            # base65536 gives single character for 16bits == 2bytes
            # use ascii base instead of base65536.encode since misplaced cursor when concealing wide characters
            # https://github.com/neovim/neovim/issues/15565
            # Base64 stores 6 bits per letter. 000000 is represented as 'A'
            LMDB._set_key_val(full_id, short_id_bytes, full_id_short_id_cursor, True)
            LMDB._set_key_val(short_id_bytes, full_id, short_id_bytes_full_id_cursor, True)

        short_node_id = short_id_encoder(short_id_bytes)
        return cast(NodeShortId, short_node_id)


class _DbSourceInfo(MLMDB):
    def get_source_id_info(self, source_id: SourceId) -> tuple[DbType, str]:
        source_info = tuple(LMDB._get_key_val(source_id, self._cursors.source_id_info, True, False))
        return cast(tuple[DbType, str], source_info)

    def set_source_id_info(self, source_id: SourceId, source_type: DbType, source_location: str) -> None:
        LMDB._set_key_val(source_id, [source_type, source_location], self._cursors.source_id_info, True)

    def remove_source_id_info(self, source_id: SourceId) -> None:
        LMDB._remove_key(source_id, self._cursors.source_id_info, False)


class MaDatabase(_DbNodeIds, _DbSourceInfo):
    def __init__(self, db_path: str):
        self._set_lmdb_env(db_path)
        super().__init__()
        self.private_key = self.get_set_private_key()
        self.public_key_bytes = self.private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)

    def get_set_private_key(self) -> X25519PrivateKey:
        try:
            encryption_key = X25519PrivateKey.from_private_bytes(LMDB._get_key_val(DbMetaKey.REALTIME_ENCRYPTION_KEY, self._cursors.metadata, True, True))
        except KeyNotFoundError:
            encryption_key = self.set_new_private_key()
        return encryption_key

    def set_new_private_key(self) -> X25519PrivateKey:
        private_key = X25519PrivateKey.generate()
        LMDB._set_key_val(DbMetaKey.PRIVATE_KEY, private_key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()), self._cursors.metadata, True)
        return private_key


class _DbUnsynced(LMDB, SyncableDb, ABC):
    def delete_unsynced_content_children(self, node_id: NodeId) -> None:
        for cursor in (self._cursors.unsynced_content, self._cursors.unsynced_children):
            if cursor.set_key(node_id.encode()):
                cursor.delete()

    def if_unsynced_children(self, node_id: NodeId) -> bool:
        return bool(self._cursors.unsynced_children.set_key(node_id.encode()))

    def if_unsynced_content(self, node_id: NodeId) -> bool:
        return bool(self._cursors.unsynced_content.set_key(node_id.encode()))

    def pop_unsynced_node_ids(self) -> Iterable[NodeId]:
        for node_id in set(chain(*(self._cursor_keys(cursor) for cursor in
                                   (self._cursors.unsynced_content, self._cursors.unsynced_children)))):
            self.delete_unsynced_content_children(node_id)
            yield node_id


class _DbEncryption(LMDB, ABC):
    # Single key for local and git since they are full synced (unlike realtime where only broadcasts are readable and thus require different key)
    def __init__(self) -> None:
        super().__init__()
        self.fernet = Fernet(self._get_encryption_key_bytes())

    def db_encrypted(self) -> bool:
        return bool(LMDB._get_key_val(DbMetaKey.DB_ENCRYPTION_ENABLED_KEY, self._cursors.metadata, False, False))

    @abstractmethod
    def get_database_password(self) -> str:
        pass

    def _get_db_encryption_salt(self)->bytes:
        salt = self._get_key_val(DbMetaKey.ENCRYPTION_SALT, self._cursors.metadata, False, True)
        if salt is None:
            salt = urandom(16)
            self._set_key_val(DbMetaKey.ENCRYPTION_SALT, salt, self._cursors.metadata, True)
        return salt

    def _get_encryption_key_bytes(self)->bytes:
        password = self.get_database_password().encode()
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=self._get_db_encryption_salt(), iterations=390000)
        key_bytes = urlsafe_b64encode(kdf.derive(password))
        return key_bytes

    def _toggle_encryption(self) -> None:
        encryption_existed = self.db_encrypted()
        self.encrypt_db = not encryption_existed

        for keys, temporary in ((LMDB._cursor_keys(self._cursors.content), False), (LMDB._cursor_keys(self._cursors.temp_content), True)):
            for node_id in keys:
                node_id = cast(NodeId, node_id)
                raw_content_lines = self._get_raw_node_content_lines(node_id, temporary)
                if encryption_existed:
                    raw_content_lines = cast(El, raw_content_lines)
                    content_lines = self.decrypt_lines(raw_content_lines)
                else:
                    content_lines = cast(Li, raw_content_lines)
                self.set_node_content_lines(node_id, content_lines)

        for _node_id in LMDB._cursor_keys(self._cursors.bloom_filters):
            self._cursors.bloom_filters.delete()

        LMDB._set_key_val(DbMetaKey.DB_ENCRYPTION_ENABLED_KEY, self.encrypt_db, self._cursors.metadata, True)

    def enable_encryption(self)->bool:
        if not self.db_encrypted():
            self._toggle_encryption()
            return True
        return False

    def disable_encryption(self)->bool:
        if self.db_encrypted():
            self._toggle_encryption()
            return True
        return False


class _DbContent(_DbEncryption, LMDB, MinimalDb, ABC):
    def __init__(self) -> None:
        super().__init__()
        self.encrypt_db: bool = False

    def decrypt_lines(self, encrypted_lines: El) -> Li:
        return cast(Li, self.fernet.decrypt(encrypted_lines[0].encode()).decode().split('\n'))

    def encrypt_lines(self, unencrypted_lines: Li) -> El:
        return cast(El, [self.fernet.encrypt('\n'.join(unencrypted_lines).encode()).decode()])

    def get_node_ids(self, temporary: bool) -> list[NodeId]:
        return [cast(NodeId, node_id) for node_id in LMDB._cursor_keys(self._cursors.temp_content if temporary else self._cursors.content)]

    def _get_raw_node_content_lines(self, node_id: NodeId, temporary: bool) -> Union[El, Li]:
        db_value = cast(El, LMDB._get_key_val(node_id, self._cursors.temp_content if temporary else self._cursors.content, True, False))
        return db_value

    def get_node_content_lines(self, node_id: NodeId, temporary: bool) -> Li:
        db_node_content_lines = self._get_raw_node_content_lines(node_id, temporary)
        db_value = self.decrypt_lines(cast(El, db_node_content_lines)) if self.encrypt_db else cast(Li, db_node_content_lines)
        return db_value

    def set_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        LMDB._set_key_val(node_id, self.encrypt_lines(content_lines) if self.encrypt_db else content_lines, self._cursors.content,
                          True)
        self.set_unsynced(self._cursors.unsynced_content, node_id)
        if self._cursors.bloom_filters.set_key(node_id.encode()):
            self._cursors.bloom_filters.delete()


class _DbDescendants(LMDB, MinimalDb, ABC):
    def _get_descendant_cursor(self, transposed: bool, temporary: bool) -> Cursor:
        return (self._cursors.temp_parents if transposed else self._cursors.temp_children) if temporary else (self._cursors.parents if transposed else self._cursors.children)

    def get_node_descendants(self, node_id: NodeId, transposed: bool, discard_invalid: bool, temporary: bool) -> OrderedSet[NodeId]:
        node_descendants = cast(OrderedSet[NodeId], OrderedSet(
            LMDB._get_key_val(node_id, self._get_descendant_cursor(transposed, temporary), False,
                              False) or []))
        if not discard_invalid:
            return node_descendants
        to_remove = set()
        for descendant_id in node_descendants:
            if not (self._cursors.temp_content if temporary else self._cursors.content).set_key(descendant_id.encode()):
                to_remove.add(descendant_id)
        if to_remove:
            for descendant_id in to_remove:
                self.delete_node(descendant_id)
            node_descendants.difference_update(to_remove)
            self._set_node_descendants_value(node_descendants, node_id, transposed, temporary)
        return node_descendants

    def _set_node_descendants_value(self, descendant_ids: OrderedSet[NodeId], node_id: NodeId, transposed: bool, temporary) -> None:
        LMDB._set_key_val(node_id, list(descendant_ids),
                          self._get_descendant_cursor(transposed, temporary),
                          True)
        if not transposed and not temporary:
            self.set_unsynced(self._cursors.unsynced_children, node_id)

    def set_node_descendants(self, node_id: NodeId, descendant_ids: OrderedSet[NodeId], transposed: bool, temporary: bool):
        # Order important. (get then set)
        previous_node_descendants = self.get_node_descendants(node_id, transposed, True, temporary)

        self._add_remove_ancestor(True, node_id, descendant_ids.difference(previous_node_descendants), transposed, temporary)
        self._add_remove_ancestor(False, node_id, previous_node_descendants.difference(descendant_ids), transposed, temporary)

        self._set_node_descendants_value(descendant_ids, node_id, transposed, temporary)

    def _add_remove_ancestor(self, add_or_remove: bool, ancestor_id: NodeId, descendant_ids: Iterable[NodeId], transposed: bool, temporary: bool):
        for descendant_id in descendant_ids:
            ancestor_id_list = self.get_node_descendants(descendant_id, not transposed, False, temporary)
            if add_or_remove:
                ancestor_id_list.add(ancestor_id)
            else:
                ancestor_id_list.remove(ancestor_id)
            self._set_node_descendants_value(ancestor_id_list, descendant_id, not transposed, temporary)

    def children_hash(self, node_id: NodeId, temporary: bool) -> str:
        return children_data_hash(self.get_node_descendants(node_id, False, True, temporary))


class _DbMeta(LMDB, MinimalDb, ABC):
    def get_root_id(self) -> NodeId:
        return cast(NodeId, LMDB._get_key_val(DbMetaKey.ROOT_ID_KEY, self._cursors.metadata, True, False))

    def set_root_id(self, root_id: NodeId) -> None:
        LMDB._set_key_val(DbMetaKey.ROOT_ID_KEY, root_id, self._cursors.metadata, False)

    def db_encrypted(self) -> bool:
        return bool(LMDB._get_key_val(DbMetaKey.DB_ENCRYPTION_ENABLED_KEY, self._cursors.metadata, False, False))

    def _get_source_id(self) -> SourceId:
        return cast(SourceId, LMDB._get_key_val(DbMetaKey.SOURCE_ID_KEY, self._cursors.metadata, True, False))

    def set_source_id(self, source_id: SourceId, main_db: MaDatabase) -> None:
        try:
            source_id = self._get_source_id()
        except KeyNotFoundError:
            pass
        else:
            main_db.remove_source_id_info(source_id)
        main_db.set_source_id_info(source_id, DbType.LMDB, _DB_FOLDER)
        LMDB._set_key_val(DbMetaKey.SOURCE_ID_KEY, source_id, self._cursors.metadata, True)

    def get_set_source_id(self, main_db: MaDatabase) -> SourceId:
        try:
            source_id = self._get_source_id()
        except KeyNotFoundError:
            source_id = cast(SourceId, get_time_uuid())
            self.set_source_id(source_id, main_db)
        return source_id

    def get_set_source_name(self) -> str:
        try:
            source_name = LMDB._get_key_val(DbMetaKey.SOURCE_NAME_KEY, self._cursors.metadata, True, False)
        except KeyNotFoundError:
            from secrets import token_urlsafe
            source_name = "nvim:{token_urlsafe(1)}"
            self.set_source_name(source_name)
        return source_name

    def set_source_name(self, source_name: str) -> None:
        LMDB._set_key_val(DbMetaKey.SOURCE_NAME_KEY, source_name, self._cursors.metadata, False)


class _DbHelpers(_DbContent, _DbDescendants, _DbMeta, LMDB, MutableDb, ABC):
    def ensure_root_node(self) -> None:
        try:
            self.get_root_id()
        except KeyNotFoundError:
            root_id = cast(NodeId, "017b99da-b1b5-19e9-e98d-8584cf46cfcf")  # get_time_uuid()
            self.set_node_content_lines(root_id, cast(Li, ['']))
            self.set_node_descendants(root_id, OrderedSet(), False, False)
            self.set_root_id(root_id)

    def get_node_view(self, node_id: NodeId, transposed: bool, main_db: MaDatabase) -> View:
        return View(node_id, self.get_set_source_id(main_db),
                    cast(Optional[Tree], LMDB._get_key_val(node_id, self._cursors.transposed_views if transposed else self._cursors.views, False, False)) or {},
                    transposed)

    def set_node_view(self, view: View) -> None:
        assert view.sub_tree is not None, "Who is setting None views?"
        LMDB._set_key_val(view.main_id, cast(Optional[dict[str, object]], view.sub_tree),
                          self._cursors.transposed_views if view.transposed else self._cursors.views, True)
        if not view.transposed:
            self.set_unsynced(self._cursors.unsynced_views, view.main_id)


class MiDatabase(_DbDescendants, _DbContent, _DbMeta, _DbEncryption, MinimalDb):
    def __init__(self, db_path: str, password_callback: Callable[[], str]) -> None:
        self._password_callback = password_callback
        self._set_lmdb_env(db_path)
        self._source_id = get_uuid()
        super().__init__()

    def get_database_password(self)->str:
        return self._password_callback()

    def get_set_source_id(self, _main_db: MaDatabase) -> SourceId:
        # While browsing backups source ID must be different to prevent conflicts
        return self._source_id

    def get_set_keywords(self, node_id) -> Container:
        bloom_filter_data: bytes = self._cursors.bloom_filters.get(node_id.encode())
        bloom_filter = BloomFilter.loads(fernet.decrypt(bloom_filter_data) if self.encrypt_db else bloom_filter_data
                                         ) if bloom_filter_data else normalized_search_prefixes(self.get_node_content_lines(node_id, temporary))
        return bloom_filter


class _DbBloom(_DbContent, LMDB, MinimalDb, ABC):
    def set_bloom_filter(self, node_id: NodeId, content_lines: Li) -> BloomFilter:
        bloom_filter = BloomFilter(expected_insertions=100, err_rate=0.1, strategy=MURMUR128_MITZ_32)
        string = '\n'.join(content_lines)
        prefixes = normalized_search_prefixes(string)
        for prefix in prefixes:
            bloom_filter.put(prefix)
        bloom_filter_bytes = bloom_filter.dumps()
        if self.encrypt_db:
            bloom_filter_bytes = fernet.encrypt(bloom_filter_bytes)
        self._cursors.bloom_filters.put(node_id.encode(), bloom_filter_bytes)
        return bloom_filter

    def get_set_keywords(self, node_id) -> Container:
        bloom_filter_data: bytes = self._cursors.bloom_filters.get(node_id.encode())
        bloom_filter = BloomFilter.loads(fernet.decrypt(bloom_filter_data) if self.encrypt_db else bloom_filter_data
                                         ) if bloom_filter_data else self.set_bloom_filter(node_id, self.get_node_content_lines(node_id, temporary))
        return bloom_filter


class _DbRealtimeEncryption(LMDB):
    def __init__(self, private_key: X25519PrivateKey) -> None:
        super().__init__()
        self.realtime_encryption_key = self.get_set_realtime_encryption_key()
        self.private_key = private_key

    def get_set_realtime_encryption_key(self) -> bytes:
        try:
            realtime_encryption_key = LMDB._get_key_val(DbMetaKey.REALTIME_ENCRYPTION_KEY, self._cursors.metadata, True, True)
        except KeyNotFoundError:
            realtime_encryption_key = self.set_new_realtime_encryption_key()
        return realtime_encryption_key

    def set_new_realtime_encryption_key(self) -> bytes:
        realtime_encryption_key = Fernet.generate_key()
        LMDB._set_key_val(DbMetaKey.REALTIME_ENCRYPTION_KEY, realtime_encryption_key, self._cursors.metadata, True)
        return realtime_encryption_key

    def encrypted_realtime_encryption_key(self, realtime_reciever_public_key: X25519PublicKey) -> bytes:
        derived_key = urlsafe_b64encode(HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b'realtime sync'
                                             ).derive(self.private_key.exchange(realtime_reciever_public_key)))
        f = Fernet(derived_key)
        return f.encrypt(self.realtime_encryption_key)


class _DbRtContent(LMDB, SyncableDb, ABC):
    def get_temporary_node_ids(self) -> list[NodeId]:
        return [cast(NodeId, node_id) for node_id in LMDB._cursor_keys(self._cursors.temp_content)]

    def _get_raw_temporary_node_content_lines(self, node_id: NodeId) -> El:
        db_value = cast(El, LMDB._get_key_val(node_id, self._cursors.temp_content, True, False))
        return db_value

    def get_temporary_node_content_lines(self, node_id: NodeId) -> Li:
        db_node_content_lines = self._get_raw_temporary_node_content_lines(node_id)
        db_value = self.decrypt_lines(cast(El, db_node_content_lines)) if self.encrypt_db else cast(Li, db_node_content_lines)
        return db_value

    def set_temporary_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        LMDB._set_key_val(node_id, self.encrypt_lines(content_lines) if self.encrypt_db else content_lines, self._cursors.temp_content,
                          True)
        self.set_unsynced(self._cursors.unsynced_content, node_id)
        if self._cursors.bloom_filters.set_key(node_id.encode()):
            self._cursors.bloom_filters.delete()

class _DbRtDescendants(LMDB, SyncableDb, ABC):
    def get_temporary_node_descendants(self, node_id: NodeId) -> Li:
        db_node_content_lines = self._get_raw_temporary_node_content_lines(node_id)
        db_value = self.decrypt_lines(cast(El, db_node_content_lines)) if self.encrypt_db else cast(Li, db_node_content_lines)
        return db_value

    def set_temporary_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        LMDB._set_key_val(node_id, self.encrypt_lines(content_lines) if self.encrypt_db else content_lines, self._cursors.temp_content,
                          True)
        self.set_unsynced(self._cursors.unsynced_content, node_id)
        if self._cursors.bloom_filters.set_key(node_id.encode()):
            self._cursors.bloom_filters.delete()

class MuDatabase(_DbHelpers, _DbDescendants, _DbBloom, _DbContent, _DbUnsynced, _DbMeta, MutableDb):
    def bootstrap(self) -> None:
        # TODO: Individual source based encryption
        if bool(ENCRYPT_DB) != self.db_encrypted():
            self.set_encryption()
        self.ensure_root_node()
        StartLoggedThread(target=lambda: setup_repository(self.git_repository_dir(self.main_db), self.repository_setup), name="SetupRepo", delay_seconds=0)

    def __init__(self, db_path: str):
        self._set_lmdb_env(db_path)
        super().__init__()


class Database:
    _source_type_database = {DbType.LMDB: MuDatabase}

    def _get_db(self, source_id: SourceId) -> MinimalDb:
        source_type, source_location = self.main_db.get_source_id_info(source_id)
        return Database._source_type_database[source_type](source_location)

    dbs: dict[SourceId, MinimalDb] = defaultdict(_get_db)

    def __init__(self, source_id: Optional[SourceId]) -> None:
        self._source_id = source_id

    def __exit__(self, *a) -> None:
        if self._db:
            self._db.__exit__(*a)
        self._main_db.__exit__(*a)

    def __enter__(self) -> MinimalDb:
        self._main_db = MaDatabase(_DB_FOLDER)
        self.main_db = self._main_db.__enter__()

        self._db = None if self._source_id is None else Database.dbs[self._source_id]
        return self._db.__enter__() if self._db else self.main_db
