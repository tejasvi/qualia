from itertools import chain
from typing import Union, cast, Iterable, Optional, Dict

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32
from orderedset import OrderedSet

from qualia.config import ENCRYPT_DB, _ROOT_ID_KEY, _DB_ENCRYPTION_ENABLED_KEY, _CLIENT_KEY, \
    _SHORT_ID_STORE_BYTES
from qualia.models import NodeId, El, Li, View, Tree, DbClient, ShortId, MinimalDb
from qualia.utils.common_utils import decrypt_lines, encrypt_lines, get_uuid, children_data_hash, \
    fernet, normalized_search_prefixes, buffer_id_encoder
from qualia.utils.database_utils import LMDB


class _DbUnsynced(LMDB):
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


class _DbContent(_DbUnsynced, LMDB):
    def _get_db_node_content_lines(self, node_id: NodeId) -> Union[El, Li]:
        db_value = cast(El, LMDB._get_key_val(node_id, self._cursors.content, True, False))
        return db_value

    def get_node_content_lines(self, node_id: NodeId) -> Li:
        db_node_content_lines = self._get_db_node_content_lines(node_id)
        db_value = decrypt_lines(cast(El, db_node_content_lines)) if ENCRYPT_DB else cast(Li, db_node_content_lines)
        return db_value

    def set_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        LMDB._set_key_val(node_id, encrypt_lines(content_lines) if ENCRYPT_DB else content_lines, self._cursors.content,
                          True)
        self.set_unsynced(self._cursors.unsynced_content, node_id)
        if self._cursors.bloom_filters.set_key(node_id.encode()):
            self._cursors.bloom_filters.delete()


class _DbDescendants(_DbUnsynced, LMDB):
    def get_node_descendants(self, node_id: NodeId, transposed: bool, discard_invalid: bool) -> OrderedSet[NodeId]:
        node_descendants = cast(OrderedSet[NodeId], OrderedSet(
            LMDB._get_key_val(node_id, self._cursors.parents if transposed else self._cursors.children, False,
                              False) or []))
        if not discard_invalid:
            return node_descendants
        to_remove = set()
        for descendant_id in node_descendants:
            if not self._cursors.content.set_key(descendant_id.encode()):
                to_remove.add(descendant_id)
        if to_remove:
            for descendant_id in to_remove:
                self.delete_node(descendant_id)
            node_descendants.difference_update(to_remove)
            self._set_node_descendants_value(node_descendants, node_id, transposed)
        return node_descendants

    def _set_node_descendants_value(self, descendant_ids: OrderedSet[NodeId], node_id: NodeId,
                                    transposed: bool) -> None:
        LMDB._set_key_val(node_id, list(descendant_ids),
                          self._cursors.parents if transposed else self._cursors.children,
                          True)
        if not transposed:
            self.set_unsynced(self._cursors.unsynced_children, node_id)

    def set_node_descendants(self, node_id: NodeId, descendant_ids: OrderedSet[NodeId], transposed: bool):
        # Order important. (get then set)
        previous_node_descendants = self.get_node_descendants(node_id, transposed, True)

        self._add_remove_ancestor(True, node_id, descendant_ids.difference(previous_node_descendants), transposed)
        self._add_remove_ancestor(False, node_id, previous_node_descendants.difference(descendant_ids), transposed)

        self._set_node_descendants_value(descendant_ids, node_id, transposed)

    def _add_remove_ancestor(self, add_or_remove: bool, ancestor_id: NodeId, descendant_ids: Iterable[NodeId],
                             transposed: bool):
        for descendant_id in descendant_ids:
            ancestor_id_list = self.get_node_descendants(descendant_id, not transposed, False)
            if add_or_remove:
                ancestor_id_list.add(ancestor_id)
            else:
                ancestor_id_list.remove(ancestor_id)
            self._set_node_descendants_value(ancestor_id_list, descendant_id, not transposed)

    def children_hash(self, node_id: NodeId) -> str:
        return children_data_hash(self.get_node_descendants(node_id, False, True))


class _DbView(LMDB):
    def get_node_view(self, node_id: NodeId, transposed: bool) -> View:
        return View(node_id, cast(Optional[Tree], LMDB._get_key_val(node_id,
                                                                    self._cursors.transposed_views if transposed else self._cursors.views,
                                                                    False, False)) or {}, transposed)

    def set_node_view(self, view: View, transposed: bool) -> None:
        LMDB._set_key_val(view.main_id, cast(Optional[dict[str, object]], view.sub_tree),
                          self._cursors.transposed_views if transposed else self._cursors.views, True)
        if not transposed:
            self.set_unsynced(self._cursors.unsynced_views, view.main_id)


class _DbNodeIds(LMDB):
    def buffer_id_bytes_to_node_id(self, buffer_id_bytes) -> NodeId:
        return cast(NodeId, LMDB._get_key_val(buffer_id_bytes, self._cursors.buffer_id_bytes_node_id, True, False))

    def node_to_buffer_id(self, node_id: NodeId) -> ShortId:
        buffer_id_bytes = LMDB._get_key_val(node_id, self._cursors.node_id_buffer_id, False, True)
        if buffer_id_bytes is None:
            if self._cursors.buffer_id_bytes_node_id.last():
                last_buffer_id_bytes = self._cursors.buffer_id_bytes_node_id.key()
                new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
                buffer_id_bytes = new_counter.to_bytes(_SHORT_ID_STORE_BYTES, 'big')
            else:
                buffer_id_bytes = (0).to_bytes(_SHORT_ID_STORE_BYTES, 'big')
            # base65536 doesn't output brackets https://qntm.org/safe
            # base65536 gives single character for 16bits == 2bytes
            # use ascii base instead of base65536.encode since misplaced cursor when concealing wide characters
            # https://github.com/neovim/neovim/issues/15565
            # Base64 stores 6 bits per letter. 000000 is represented as 'A'
            LMDB._set_key_val(node_id, buffer_id_bytes, self._cursors.node_id_buffer_id, True)
            LMDB._set_key_val(buffer_id_bytes, node_id, self._cursors.buffer_id_bytes_node_id, True)

        buffer_node_id = buffer_id_encoder(buffer_id_bytes)
        return cast(ShortId, buffer_node_id)

    def get_node_ids(self) -> list[NodeId]:
        self._cursors.content.first()
        return [cast(NodeId, node_id) for node_id in LMDB._cursor_keys(self._cursors.content)]


class _DbMeta(LMDB):
    def get_root_id(self) -> NodeId:
        return cast(NodeId, LMDB._get_key_val(_ROOT_ID_KEY, self._cursors.metadata, True, False))

    def set_root_id(self, root_id: NodeId) -> None:
        LMDB._set_key_val(_ROOT_ID_KEY, root_id, self._cursors.metadata, False)

    def db_encrypted(self) -> bool:
        return bool(LMDB._get_key_val(_DB_ENCRYPTION_ENABLED_KEY, self._cursors.metadata, False, False))

    def get_set_client(self) -> DbClient:
        db_client_data = cast(dict, LMDB._get_key_val(_CLIENT_KEY, self._cursors.metadata, False, False))
        if db_client_data is None:
            from secrets import token_urlsafe
            client_details = DbClient(client_id=str(get_uuid()), client_name=f"nvim:{token_urlsafe(1)}")
            LMDB._set_key_val(_CLIENT_KEY, cast(Dict, client_details), self._cursors.metadata, False)
        else:
            client_details = DbClient(client_id=db_client_data["client_id"], client_name=db_client_data["client_name"])
        return client_details


class _DbBloom(_DbContent, LMDB):
    def set_bloom_filter(self, node_id: NodeId, content_lines: Li) -> BloomFilter:
        bloom_filter = BloomFilter(expected_insertions=100, err_rate=0.1, strategy=MURMUR128_MITZ_32)
        string = '\n'.join(content_lines)
        prefixes = normalized_search_prefixes(string)
        for prefix in prefixes:
            bloom_filter.put(prefix)
        bloom_filter_bytes = bloom_filter.dumps()
        if ENCRYPT_DB:
            bloom_filter_bytes = fernet.encrypt(bloom_filter_bytes)
        self._cursors.bloom_filters.put(node_id.encode(), bloom_filter_bytes)
        return bloom_filter

    def get_set_bloom_filter(self, node_id) -> BloomFilter:
        bloom_filter_data: bytes = self._cursors.bloom_filters.get(node_id.encode())
        bloom_filter = BloomFilter.loads(fernet.decrypt(bloom_filter_data) if ENCRYPT_DB else
                                         bloom_filter_data) if bloom_filter_data else self.set_bloom_filter(
            node_id, self.get_node_content_lines(node_id))
        return bloom_filter


class Database(_DbDescendants, _DbBloom, _DbContent, _DbUnsynced, _DbMeta, _DbView, _DbNodeIds, MinimalDb):
    def set_encryption(self) -> None:
        encryption_existed = self.db_encrypted()
        for node_id in LMDB._cursor_keys(self._cursors.content):
            node_id = cast(NodeId, node_id)
            db_content_lines = self._get_db_node_content_lines(node_id)
            if encryption_existed:
                db_content_lines = cast(El, db_content_lines)
                content_lines = decrypt_lines(db_content_lines)
            else:
                content_lines = cast(Li, db_content_lines)
            self.set_node_content_lines(node_id, content_lines)
        for _node_id in LMDB._cursor_keys(self._cursors.bloom_filters):
            self._cursors.bloom_filters.delete()
        LMDB._set_key_val(_DB_ENCRYPTION_ENABLED_KEY, not encryption_existed, self._cursors.metadata, True)

    pass
