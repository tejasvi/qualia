from os.path import basename
from typing import Union

import lmdb
from orderedset import OrderedSet
from pynvim.api import Buffer

from qualia import states
from qualia.buffer import Process
from qualia.models import View, ProcessState, NodeId
from qualia.utils import put_key_val, get_key_val, conflict, create_root_if_new


def sync_buffer(buffer: Buffer) -> View:
    root_id = NodeId(basename(buffer.name))  # .rsplit('.q.md', maxsplit=1)
    create_root_if_new(root_id)
    if root_id in states.ledger:
        root_view, changes = Process().process_lines(list(buffer), root_id)
        sync_with_db(root_view, changes)
    else:
        root_view = View(root_id, get_key_val(root_id, states.cursors.views) or {})
    return root_view


def sync_with_db(root_view: View, changes: ProcessState):
    # Need to check with only changed nodes and no need to check with remaining nodes in ledger since only View is
    # finally what gets rendered directly from db. First sync from buffer to db then render from db.
    cursors = states.cursors
    sync_content(changes.changed_content_map, cursors.content, cursors.unsynced_content)
    sync_children(changes.changed_children_map, cursors.children, cursors.unsynced_children)

    put_key_val(root_view.root_id, root_view.sub_tree, cursors.views)
    put_key_val(root_view.root_id, True, cursors.unsynced_views)


def sync_children(changed_children_map: dict[NodeId, OrderedSet], children_cur: lmdb.Cursor,
                  unsynced_children_cur: lmdb.Cursor) -> None:
    for node_id, children_ids in changed_children_map.items():
        db_children_id_list: Union[None, list[str]] = get_key_val(node_id, children_cur)
        if db_children_id_list is not None:
            if node_id not in states.ledger or (frozenset(db_children_id_list) != states.ledger[node_id].children_ids):
                children_ids.update(db_children_id_list)
        put_key_val(node_id, list(children_ids), children_cur)
        put_key_val(node_id, True, unsynced_children_cur)


def sync_content(changed_content_map: dict[NodeId, list[str]], content_cur: lmdb.Cursor,
                 unsynced_content_cur: lmdb.Cursor) -> None:
    for node_id, content_lines in changed_content_map.items():
        db_content_lines: Union[None, list[str]] = get_key_val(node_id, content_cur)
        if db_content_lines is not None:
            if node_id not in states.ledger or db_content_lines != states.ledger[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines, False)
        put_key_val(node_id, content_lines, content_cur)
        put_key_val(node_id, True, unsynced_content_cur)
