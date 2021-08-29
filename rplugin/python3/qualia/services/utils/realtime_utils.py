from typing import Iterable, Optional, TYPE_CHECKING, cast

from lmdb import Cursor
from orderedset import OrderedSet

from qualia.models import RealtimeData, NodeId, Cursors, ConflictData, ChildrenConflictData, ContentConflictData
from qualia.utils.common_utils import conflict, set_node_content_lines, logger, \
    realtime_data_hash, set_ancestor_descendants, StartLoggedThread, get_node_descendants, get_node_content


def value_hash(key: str, cursor: Cursor) -> Optional[str]:
    data_bytes = cursor.get(key.encode())
    return None if data_bytes is None else realtime_data_hash(data_bytes)


def sync_with_realtime_db(data: RealtimeData, realtime_session) -> None:
    if data and realtime_session.others_online:
        def broadcast_closure() -> None:
            realtime_session.client_broadcast(data)

        StartLoggedThread(target=broadcast_closure, name="ClientBroadcast")


def merge_children_with_local(node_id: NodeId, new_children_ids: Iterable[NodeId], cursors: Cursors) -> list[
    NodeId]:
    merged_children_ids = get_node_descendants(cursors, node_id, False)
    merged_children_ids.update(new_children_ids)
    return list(merged_children_ids)


def merge_content_with_local(node_id: NodeId, new_content_lines: list[str], cursors: Cursors) -> list[str]:
    db_content_lines: list[str] = get_node_content(cursors, node_id)
    return new_content_lines if db_content_lines is None else conflict(new_content_lines, db_content_lines)


def _process_broadcast_data(data_dict: ConflictData, cursors: Cursors, is_content_or_children: bool) -> ConflictData:
    if TYPE_CHECKING:
        conflict_data_type = ContentConflictData if is_content_or_children else ChildrenConflictData
        data_dict = cast(conflict_data_type, data_dict)
    conflicts: ConflictData = {}
    for item in data_dict.items():
        try:
            node_id, (last_hash, downstream_data) = item
            node_id: NodeId
        except ValueError:
            logger.critical("[Realtime Sync] Got corrupt value: ", item)
        else:
            db_hash: Optional[str] = value_hash(node_id,
                                                cursors.content if is_content_or_children else cursors.children)
            downstream_hash = realtime_data_hash(downstream_data)
            if downstream_hash != db_hash:  # Check spurious rebroadcasts
                if db_hash is not None and db_hash != last_hash:
                    new_data = merge_content_with_local(node_id, downstream_data, cursors
                                                        ) if is_content_or_children else merge_children_with_local(
                        node_id, downstream_data, cursors)
                    conflicts[node_id] = downstream_hash, new_data
                    downstream_data = new_data
                if is_content_or_children:
                    set_node_content_lines(downstream_data, cursors, node_id)
                else:
                    set_ancestor_descendants(cursors, OrderedSet(downstream_data), node_id, False)
    return conflicts


def process_children_broadcast(data_dict: ChildrenConflictData, cursors: Cursors) -> ChildrenConflictData:
    return cast(ChildrenConflictData, _process_broadcast_data(data_dict, cursors, False))


def process_content_broadcast(data_dict: ContentConflictData, cursors: Cursors) -> ContentConflictData:
    return cast(ContentConflictData, _process_broadcast_data(data_dict, cursors, True))
