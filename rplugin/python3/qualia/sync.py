from __future__ import annotations

from typing import Union, Optional

from lmdb import Cursor
from orderedset import OrderedSet

from qualia.buffer import Process
from qualia.models import View, ProcessState, NodeId, Cursors, LastSeen, RealtimeChildrenData, RealtimeData, \
    RealtimeContentData
from qualia.realtime import Realtime
from qualia.search import save_bloom_filter
from qualia.utils.common_utils import get_key_val, put_key_val, conflict
from qualia.utils.realtime_utils import realtime_data_hash, sync_with_realtime_db
from qualia.utils.sync_utils import add_remove_parent


def sync_buffer(buffer_lines: list[str], main_id: NodeId, last_seen: LastSeen, cursors: Cursors,
                transposed: bool, realtime_session: Realtime) -> View:
    if main_id in last_seen:
        main_view, changes = Process().process_lines(buffer_lines, main_id, last_seen, cursors)
        realtime_data = sync_with_db(main_view, changes, last_seen, cursors, transposed, realtime_session.others_online)
        sync_with_realtime_db(realtime_data, realtime_session)
    else:
        main_view = View(main_id, get_key_val(main_id, cursors.transposed_views if transposed else cursors.views) or {})
    return main_view


def sync_with_db(root_view: Optional[View], changes: ProcessState, last_seen: LastSeen, cursors: Cursors,
                 transposed: bool, realtime: bool) -> RealtimeData:
    if root_view:
        save_root_view(root_view, cursors.views)
        put_key_val(root_view.main_id, True, cursors.unsynced_views, True)

    # Need to check with only changed nodes and no need to check with remaining nodes in last_seen since only View is
    # finally what gets rendered directly from db. First sync buffer to db then render from db.
    realtime_content_data = sync_content(changes.changed_content_map, cursors, last_seen, realtime)

    children_cur, parents_cur = cursors.children, cursors.parents
    unsynced_children_cur = cursors.unsynced_children
    if transposed:
        #
        parents_cur, children_cur = children_cur, parents_cur
        unsynced_children_cur = None

    realtime_children_data = sync_children(changes.changed_children_map, children_cur, unsynced_children_cur,
                                           last_seen, parents_cur, realtime)
    return {"content": realtime_content_data, "children": realtime_children_data}


def save_root_view(view: View, views_cur: Cursor) -> None:
    put_key_val(view.main_id, view.sub_tree, views_cur, True)


def sync_children(changed_children_map: dict[NodeId, OrderedSet[NodeId]], children_cur: Cursor,
                  unsynced_children_cur: Optional[Cursor],
                  last_seen: LastSeen, parents_cur: Cursor, realtime) -> RealtimeChildrenData:
    realtime_children_data: RealtimeChildrenData = {}
    for node_id, children_ids in changed_children_map.items():
        db_children_ids: frozenset[NodeId] = frozenset(get_key_val(node_id, children_cur) or [])
        if node_id not in last_seen or (db_children_ids != last_seen[node_id].children_ids):
            children_ids.update(db_children_ids)

        put_key_val(node_id, list(children_ids), children_cur, True)

        if realtime:
            realtime_children_data[node_id] = realtime_data_hash(list(db_children_ids)), list(children_ids)

        if unsynced_children_cur:
            put_key_val(node_id, True, unsynced_children_cur, True)

        add_remove_parent(True, node_id, children_ids.difference(db_children_ids), parents_cur, unsynced_children_cur)
        add_remove_parent(False, node_id, db_children_ids.difference(children_ids), parents_cur, unsynced_children_cur)
    return realtime_children_data


def sync_content(changed_content_map: dict[NodeId, list[str]], cursors: Cursors, last_seen: LastSeen,
                 realtime) -> RealtimeContentData:
    realtime_content_data: RealtimeContentData = {}
    for node_id, content_lines in changed_content_map.items():
        db_content_lines: Union[None, list[str]] = get_key_val(node_id, cursors.content)
        if db_content_lines is not None:
            if node_id not in last_seen or db_content_lines != last_seen[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines)
        put_key_val(node_id, content_lines, cursors.content, True)

        if realtime:
            realtime_content_data[node_id] = realtime_data_hash(db_content_lines), content_lines

        save_bloom_filter(node_id, content_lines, cursors.bloom_filters)

        put_key_val(node_id, True, cursors.unsynced_content, True)
    return realtime_content_data
