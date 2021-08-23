from __future__ import annotations

from typing import Union, Optional, Iterable

from lmdb import Cursor
from orderedset import OrderedSet

from qualia.buffer import Process
from qualia.models import View, ProcessState, NodeId, Cursors, LastSeen, RealtimeChildrenData, RealtimeData, \
    RealtimeContentData
from qualia.realtime import Realtime
from qualia.search import save_bloom_filter
from qualia.utils import put_key_val, get_key_val, conflict, realtime_data_hash, sync_with_realtime_db


def sync_buffer(buffer_lines: list[str], main_id: NodeId, last_seen: LastSeen, cursors: Cursors,
                inverted: bool, realtime_session: Realtime) -> View:
    if main_id in last_seen:
        main_view, changes = Process().process_lines(buffer_lines, main_id, last_seen)
        realtime_data = sync_with_db(main_view, changes, last_seen, cursors, inverted, realtime_session.others_online)
        sync_with_realtime_db(realtime_data, realtime_session)
    else:
        main_view = View(main_id, get_key_val(main_id, cursors.inverted_views if inverted else cursors.views) or {})
    return main_view


def sync_with_db(root_view: Optional[View], changes: ProcessState, last_seen: LastSeen, cursors: Cursors,
                 inverted: bool, realtime: bool) -> RealtimeData:
    if root_view:
        put_key_val(root_view.main_id, root_view.sub_tree, cursors.views, True)
        put_key_val(root_view.main_id, True, cursors.unsynced_views, True)

    # Need to check with only changed nodes and no need to check with remaining nodes in last_seen since only View is
    # finally what gets rendered directly from db. First sync buffer to db then render from db.
    realtime_content_data = sync_content(changes.changed_content_map, cursors, last_seen, realtime)
    children_cur, parents_cur = cursors.children, cursors.parents
    if inverted:
        parents_cur, children_cur = children_cur, parents_cur
    realtime_children_data = sync_children(changes.changed_children_map, children_cur, cursors.unsynced_children,
                                           last_seen, parents_cur, inverted, realtime)
    return {"content": realtime_content_data, "children": realtime_children_data}


def sync_children(changed_children_map: dict[NodeId, OrderedSet[NodeId]], children_cur: Cursor,
                  unsynced_children_cur: Cursor,
                  last_seen: LastSeen, parents_cur: Cursor, inverted: bool, realtime) -> RealtimeChildrenData:
    realtime_children_data: RealtimeChildrenData = {}
    for node_id, children_ids in changed_children_map.items():
        db_children_ids: frozenset[NodeId] = frozenset(get_key_val(node_id, children_cur) or [])
        if node_id not in last_seen or (db_children_ids != last_seen[node_id].children_ids):
            children_ids.update(db_children_ids)

        put_key_val(node_id, list(children_ids), children_cur, True)

        if realtime:
            realtime_children_data[node_id] = realtime_data_hash(list(db_children_ids)), list(children_ids)

        if not inverted:
            put_key_val(node_id, True, unsynced_children_cur, True)

        add_remove_parent(True, node_id, children_ids.difference(db_children_ids), parents_cur, unsynced_children_cur,
                          inverted)
        add_remove_parent(False, node_id, db_children_ids.difference(children_ids), parents_cur, unsynced_children_cur,
                          inverted)
    return realtime_children_data


def add_remove_parent(add: bool, parent_id: NodeId, children_ids: Iterable[NodeId], parents_cur: Cursor,
                      unsynced_children_cur: Cursor, inverted: bool):
    for children_id in children_ids:
        parent_id_list: list[str] = get_key_val(children_id, parents_cur) or []
        if add:
            parent_id_list.append(parent_id)
        else:
            parent_id_list.remove(parent_id)
        put_key_val(children_id, parent_id_list, parents_cur, True)

        if inverted:
            put_key_val(children_id, True, unsynced_children_cur, True)


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
