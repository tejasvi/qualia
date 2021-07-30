from typing import Union, Optional

import lmdb
from orderedset import OrderedSet
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.models import View, ProcessState, NodeId, Cursors, LastSeen
from qualia.utils import put_key_val, get_key_val, conflict, get_main_id


def sync_buffer(buffer: Buffer, last_seen: LastSeen, cursors: Cursors) -> View:
    main_id = get_main_id(buffer, cursors)
    if main_id in last_seen:
        root_view, changes = Process().process_lines(list(buffer), main_id, last_seen)
        sync_with_db(root_view, changes, last_seen, cursors)
    else:
        root_view = View(main_id, get_key_val(main_id, cursors.views) or {})
    return root_view


def sync_with_db(root_view: Optional[View], changes: ProcessState, last_seen: LastSeen, cursors: Cursors):
    # Need to check with only changed nodes and no need to check with remaining nodes in last_seen since only View is
    # finally what gets rendered directly from db. First sync from buffer to db then render from db.
    sync_content(changes.changed_content_map, cursors.content, cursors.unsynced_content, last_seen)
    sync_children(changes.changed_children_map, cursors.children, cursors.unsynced_children, last_seen)

    if root_view:
        put_key_val(root_view.main_id, root_view.sub_tree, cursors.views, True)
        put_key_val(root_view.main_id, True, cursors.unsynced_views, True)


def sync_children(changed_children_map: dict[NodeId, OrderedSet], children_cur: lmdb.Cursor,
                  unsynced_children_cur: lmdb.Cursor, last_seen: LastSeen) -> None:
    for node_id, children_ids in changed_children_map.items():
        db_children_id_list: Union[None, list[str]] = get_key_val(node_id, children_cur)
        if db_children_id_list is not None:
            if node_id not in last_seen or (frozenset(db_children_id_list) != last_seen[node_id].children_ids):
                children_ids.update(db_children_id_list)

        put_key_val(node_id, list(children_ids), children_cur, True)
        put_key_val(node_id, True, unsynced_children_cur, True)


def sync_content(changed_content_map: dict[NodeId, list[str]], content_cur: lmdb.Cursor,
                 unsynced_content_cur: lmdb.Cursor, last_seen: LastSeen) -> None:
    for node_id, content_lines in changed_content_map.items():
        db_content_lines: Union[None, list[str]] = get_key_val(node_id, content_cur)
        if db_content_lines is not None:
            if node_id not in last_seen or db_content_lines != last_seen[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines, False)
        put_key_val(node_id, content_lines, content_cur, True)
        put_key_val(node_id, True, unsynced_content_cur, True)
