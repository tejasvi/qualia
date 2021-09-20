from base64 import b64encode
from secrets import token_urlsafe
from typing import Union, cast, Iterable, Optional, Dict

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32
from orderedset import OrderedSet

from qualia.config import ENCRYPT_DB, _ROOT_ID_KEY, _DB_ENCRYPTION_ENABLED_KEY, _CLIENT_KEY, \
    _SHORT_BUFFER_ID, _BUFFER_ID_STORE_BYTES
from qualia.models import NodeId, El, Li, View, Tree, DbClient, BufferNodeId
from qualia.utils.common_utils import decrypt_lines, encrypt_lines, _get_uuid, logger, children_data_hash, \
    fernet, normalized_search_prefixes
from qualia.utils.database_utils import _get_key_val, _set_key_val, _pop_if_exists, _cursor_keys, _LMDB


class _DbDescendants(_LMDB):
    def get_node_descendants(self, node_id: NodeId, transposed: bool, discard_invalid: bool) -> OrderedSet[NodeId]:
        node_descendants = cast(OrderedSet[NodeId], OrderedSet(
            _get_key_val(node_id, self._cursors.parents if transposed else self._cursors.children, False) or []))
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
        _set_key_val(node_id, list(descendant_ids), self._cursors.parents if transposed else self._cursors.children,
                     True)
        if not transposed:
            _set_key_val(node_id, True, self._cursors.unsynced_children, True)

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


class _DbContent(_LMDB):
    def _get_db_node_content_lines(self, node_id: NodeId) -> Union[El, Li]:
        db_value = cast(El, _get_key_val(node_id, self._cursors.content, True))
        return db_value

    def get_node_content_lines(self, node_id: NodeId) -> Li:
        db_node_content_lines = self._get_db_node_content_lines(node_id)
        db_value = decrypt_lines(cast(El, db_node_content_lines)) if ENCRYPT_DB else cast(Li, db_node_content_lines)
        return db_value

    def set_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        _set_key_val(node_id, encrypt_lines(content_lines) if ENCRYPT_DB else content_lines, self._cursors.content,
                     True)
        _set_key_val(node_id, True, self._cursors.unsynced_content, True)
        if self._cursors.bloom_filters.set_key(node_id.encode()):
            self._cursors.bloom_filters.delete()

    def toggle_encryption(self) -> None:
        for node_id in _cursor_keys(self._cursors.content):
            node_id = cast(NodeId, node_id)
            db_content_lines = self._get_db_node_content_lines(node_id)
            if ENCRYPT_DB:
                content_lines = cast(Li, db_content_lines)
            else:
                db_content_lines = cast(El, db_content_lines)
                content_lines = decrypt_lines(db_content_lines)
            self.set_node_content_lines(node_id, content_lines)
        for _node_id in _cursor_keys(self._cursors.bloom_filters):
            self._cursors.bloom_filters.delete()
        _set_key_val(_DB_ENCRYPTION_ENABLED_KEY, bool(ENCRYPT_DB), self._cursors.metadata, True)


class _DBView(_LMDB):
    def get_node_view(self, node_id: NodeId, transposed: bool) -> View:
        return View(node_id, cast(Optional[Tree], _get_key_val(
            node_id, self._cursors.transposed_views if transposed else self._cursors.views, False)) or {})

    def set_node_view(self, view: View, transposed: bool) -> None:
        _set_key_val(view.main_id, cast(Optional[dict[str, object]], view.sub_tree),
                     self._cursors.transposed_views if transposed else self._cursors.views, True)
        if not transposed:
            _set_key_val(view.main_id, True, self._cursors.unsynced_views, True)


class _DbUnsynced(_LMDB):
    def delete_unsynced_content_children(self, node_id: NodeId) -> None:
        for cursor in (self._cursors.unsynced_content, self._cursors.unsynced_children):
            if cursor.set_key(node_id.encode()):
                cursor.delete()

    def pop_if_unsynced_children(self, node_id: NodeId) -> bool:
        return _pop_if_exists(self._cursors.unsynced_children, node_id)

    def pop_if_unsynced_content(self, node_id: NodeId) -> bool:
        return _pop_if_exists(self._cursors.unsynced_content, node_id)


class _DbNodeIds(_LMDB):
    def buffer_id_bytes_to_node_id(self, buffer_id_bytes) -> NodeId:
        return cast(NodeId, _get_key_val(buffer_id_bytes, self._cursors.buffer_id_bytes_node_id, True))

    def node_to_buffer_id(self, node_id: NodeId) -> BufferNodeId:
        if not _SHORT_BUFFER_ID:
            return cast(BufferNodeId, node_id)
        buffer_node_id = cast(Optional[BufferNodeId], _get_key_val(node_id, self._cursors.node_id_buffer_id, False))
        if buffer_node_id is None:
            if self._cursors.buffer_id_bytes_node_id.last():
                last_buffer_id_bytes = self._cursors.buffer_id_bytes_node_id.key()
                new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
                buffer_id_bytes = new_counter.to_bytes(_BUFFER_ID_STORE_BYTES, 'big')
            else:
                buffer_id_bytes = (0).to_bytes(_BUFFER_ID_STORE_BYTES, 'big')
            # base65536 doesn't output brackets https://qntm.org/safe
            # base65536 gives single character for 16bits == 2bytes
            # use base64 instead of base65536.encode since misplaced cursor when concealing wide characters
            # https://github.com/neovim/neovim/issues/15565
            # Base64 stores 6 bits per letter. 000000 is represented as 'A'
            buffer_node_id = cast(BufferNodeId, b64encode(buffer_id_bytes).decode().rstrip("=").lstrip('A') or 'A')
            logger.debug(f"{node_id} {buffer_node_id}")
            _set_key_val(node_id, buffer_node_id, self._cursors.node_id_buffer_id, True)
            _set_key_val(buffer_id_bytes, node_id, self._cursors.buffer_id_bytes_node_id, True)
        return cast(BufferNodeId, buffer_node_id)

    def get_node_ids(self) -> list[NodeId]:
        self._cursors.content.first()
        return [cast(NodeId, node_id) for node_id in _cursor_keys(self._cursors.content)]


class _DbMeta(_LMDB):
    def get_root_id(self) -> NodeId:
        return cast(NodeId, _get_key_val(_ROOT_ID_KEY, self._cursors.metadata, True))

    def set_root_id(self, root_id: NodeId) -> None:
        _set_key_val(_ROOT_ID_KEY, root_id, self._cursors.metadata, False)

    def db_encrypted(self) -> bool:
        return bool(_get_key_val(_DB_ENCRYPTION_ENABLED_KEY, self._cursors.metadata, False))

    def get_set_client(self) -> DbClient:
        db_client_data = cast(dict, _get_key_val(_CLIENT_KEY, self._cursors.metadata, False))
        if db_client_data is None:
            client_details = DbClient(client_id=str(_get_uuid()), client_name=f"nvim:{token_urlsafe(1)}")
            _set_key_val(_CLIENT_KEY, cast(Dict, client_details), self._cursors.metadata, False)
        else:
            client_details = DbClient(client_id=db_client_data["client_id"], client_name=db_client_data["client_name"])
        return client_details


class _DbBloom(_LMDB):
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


class _DbUtils(_DbContent, _DbBloom):
    def get_set_bloom_filter(self, node_id) -> BloomFilter:
        bloom_filter_data: bytes = self._cursors.bloom_filters.get(node_id.encode())
        bloom_filter = BloomFilter.loads(fernet.decrypt(bloom_filter_data) if ENCRYPT_DB else
                                         bloom_filter_data) if bloom_filter_data else self.set_bloom_filter(
            node_id, self.get_node_content_lines(node_id))
        return bloom_filter


class Database(_DbUtils, _DbDescendants, _DbContent, _DbUnsynced, _DbMeta, _DBView, _DbNodeIds, _DbBloom):
    pass
    pass
