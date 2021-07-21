from typing import Union

import lmdb
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.models import View, ProcessState, NodeId
from qualia.states import ledger
from qualia.utils import put_key_val, get_key_val, conflict, Cursors


def sync_buffer(buffer: Buffer, cursors: Cursors, buffer_name) -> View:
    root_view, changes = Process().process_lines(list(buffer), buffer_name)
    sync_with_db(root_view, changes, cursors)
    return root_view


def sync_with_db(root_view: View, changes: ProcessState, cursors: Cursors):
    # Need to check with only changed nodes and no need to check with remaining nodes in ledger since only View is
    # finally what gets rendered directly from db. First sync from buffer to db then render from db.
    sync_content(changes.changed_content_map, cursors.content)
    sync_children(changes.changed_children_map, cursors.children)
    put_key_val(root_view.root_id, root_view.sub_tree, cursors.views)


def sync_children(changed_children_map: dict[NodeId, set], children_cur: lmdb.Cursor) -> None:
    for node_id, children_ids in changed_children_map.items():
        db_children_id_list: Union[None, list[str]] = get_key_val(node_id, children_cur)
        if db_children_id_list is not None:
            if node_id not in ledger or frozenset(db_children_id_list) != ledger[node_id].children_ids:
                children_ids.update(db_children_id_list)
        put_key_val(node_id, list(children_ids), children_cur)


def sync_content(changed_content_map: dict[NodeId, list[str]], content_cur: lmdb.Cursor) -> None:
    for node_id, content_lines in changed_content_map.items():
        db_content_lines: Union[None, list[str]] = get_key_val(node_id, content_cur)
        if db_content_lines is not None:
            if node_id not in ledger or db_content_lines != ledger[node_id].content_lines:
                content_lines = conflict(content_lines, db_content_lines, False)
        put_key_val(node_id, content_lines, content_cur)
