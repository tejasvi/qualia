from typing import Optional

from orderedset import OrderedSet

from qualia.models import View, ProcessState, LastSync, Cursors, RealtimeBroadcastPacket, NodeId, RealtimeChildren, Li, \
    RealtimeContent, KeyNotFoundError
from qualia.utils.common_utils import save_root_view, set_key_val, get_node_descendants, children_hash, \
    ordered_data_hash, set_node_descendants, get_node_content_lines, conflict, set_node_content_lines, \
    children_data_hash, absent_node_content_lines


def sync_with_db(root_view: Optional[View], changes: ProcessState, last_sync: LastSync, cursors: Cursors,
                 transposed: bool, realtime: bool) -> RealtimeBroadcastPacket:
    if root_view:
        save_root_view(root_view, cursors.transposed_views if transposed else cursors.views)
        if not transposed:
            set_key_val(root_view.main_id, True, cursors.unsynced_views, True)

    realtime_content_data = sync_content(changes.changed_content_map, cursors, last_sync, realtime)
    realtime_children_data = sync_descendants(changes.changed_descendants_map, cursors, last_sync, transposed,
                                              realtime)
    return {"content": realtime_content_data, "children": realtime_children_data} if realtime else {}


def sync_descendants(changed_descendants_map: dict[NodeId, OrderedSet[NodeId]], cursors: Cursors,
                     last_sync: LastSync, transposed: bool, realtime: bool) -> RealtimeChildren:
    new_descendants = []
    realtime_children_data = {}
    _dummy_children: list[NodeId] = []

    for node_id, descendants_ids in changed_descendants_map.items():
        db_descendants_ids = get_node_descendants(cursors, node_id, transposed, False)
        if node_id not in last_sync or (db_descendants_ids != last_sync[node_id].descendants_ids):
            descendants_ids.update(db_descendants_ids)

        new_descendants.append((node_id, descendants_ids))

        if realtime:
            for parent_id in descendants_ids if transposed else [node_id]:
                last_children_hash = children_hash(node_id, cursors) if transposed else children_data_hash(
                    db_descendants_ids)  # Save db lookup
                realtime_children_data[parent_id] = last_children_hash, _dummy_children if transposed else list(
                    descendants_ids)

    for node_id, descendants_ids in new_descendants:
        set_node_descendants(node_id, descendants_ids, cursors, transposed)

    if realtime and transposed:
        for parent_id, (last_hash, _dummy_children) in realtime_children_data.items():
            new_children_node_ids = list(get_node_descendants(cursors, parent_id, False, False))
            realtime_children_data[parent_id] = last_hash, new_children_node_ids

    return realtime_children_data


def sync_content(changed_content_map: dict[NodeId, Li], cursors: Cursors, last_sync: LastSync,
                 realtime) -> RealtimeContent:
    realtime_content_data: RealtimeContent = {}
    for node_id, content_lines in changed_content_map.items():
        overriden_lines = absent_node_content_lines
        try:
            db_content_lines = get_node_content_lines(cursors, node_id)
        except KeyNotFoundError:
            pass
        else:
            if node_id not in last_sync or db_content_lines != last_sync[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines)
            overriden_lines = db_content_lines
        if realtime:
            realtime_content_data[node_id] = ordered_data_hash(overriden_lines), content_lines
        set_node_content_lines(node_id, content_lines, cursors)

    return realtime_content_data
