from base64 import urlsafe_b64encode
from hashlib import sha256
from json import loads, dumps
from re import compile
from secrets import token_urlsafe
from time import time_ns
from typing import Callable, Union
from uuid import uuid4

import lmdb
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode
from pynvim import Nvim

from qualia import DuplicateException
from qualia import states
from qualia.config import DB_FOLDER, LEVEL_SPACES, EXPANDED_BULLET, COLLAPSED_BULLET, TO_EXPAND_BULLET
from qualia.models import NodeId, JSONType, BufferNodeId, NODE_ID_ATTR, Tree, Cursors, CloneChildrenException, Ledger

_md_parser = MarkdownIt().parse


def get_md_ast(content_lines: list[str]) -> SyntaxTreeNode:
    root_ast = SyntaxTreeNode(_md_parser('\n'.join(content_lines)))
    root_ast.token = Token(meta={}, map=[0, len(content_lines)], nesting=0, tag="", type="root")
    return root_ast


def get_uuid() -> NodeId:
    return NodeId(urlsafe_b64encode(uuid4().bytes).rstrip(b"=").decode())


def get_time_uuid() -> NodeId:
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    return urlsafe_b64encode(left_padded_time).decode() + token_urlsafe(10)


get_random_id: Callable[[], NodeId] = get_time_uuid


def get_node_id() -> NodeId:
    while True:
        node_id = get_random_id()
        if ")" not in node_id:
            break
    return node_id


def batch_undo(nvim: Nvim):
    first_change = True
    while True:
        if first_change:
            first_change = False
        else:
            nvim.command("undojoin")
        yield


class Database:
    def __enter__(self) -> Cursors:
        db_names = "content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views", "buffer_to_node_id", "node_to_buffer_id"
        self.env = env = lmdb.open(DB_FOLDER, max_dbs=len(db_names))
        self.txn = env.begin(write=True)
        return Cursors(**{db_name: self.sub_db(db_name) for db_name in db_names})

    def sub_db(self, db_name: str) -> lmdb.Cursor:
        return self.txn.cursor(self.env.open_db(db_name.encode(), self.txn))

    def __exit__(self, *args) -> None:
        self.txn.__exit__(*args)
        self.env.__exit__(*args)


def children_hash(children: set[NodeId]):
    return sha256('\n'.join(sorted(children)).encode()).digest()


def content_hash(content_lines: list[str]):
    return sha256('\n'.join(content_lines).encode()).digest()


def conflict(new_lines: list[str], old_lines: list[str], no_check: bool) -> list[str]:
    return new_lines + ["<!-- CONFLICT -->"] + old_lines if no_check or new_lines != old_lines else new_lines


def get_key_val(key: str, cursor: lmdb.Cursor) -> JSONType:
    value_bytes = cursor.get(key.encode())
    return None if value_bytes is None else loads(value_bytes.decode())


def put_key_val(key: str, val: JSONType, cursor: lmdb.Cursor) -> None:
    cursor.put(key.encode(), dumps(val).encode())


def node_to_buffer_id(node_id: NodeId) -> BufferNodeId:
    return BufferNodeId(node_id)
    # buffer_node_id = get_key_val(node_id, cursors.buffer_to_node_id)
    # if buffer_node_id is None:
    #     if cursors.buffer_to_node_id.last():
    #         last_buffer_id_bytes = cursors.buffer_to_node_id.key()
    #         new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
    #         buffer_id_bytes = new_counter.to_bytes(32, 'big').decode()
    #     else:
    #         buffer_id_bytes = (0).to_bytes(32, 'big')
    #     buffer_node_id = base65536.encode(buffer_id_bytes)
    #     # base65536 doesn't output brackets https://qntm.org/safe
    #     put_key_val(node_id, buffer_node_id, cursors.node_to_buffer_id)
    # return buffer_node_id


def buffer_to_node_id(buffer_id: BufferNodeId) -> Union[None, NodeId]:
    return NodeId(buffer_id)
    # buffer_id_bytes = base65536.decode(buffer_id)
    # return state.cursors.buffer_to_node_id.get(buffer_id_bytes)


def get_id_line(line: str) -> tuple[NodeId, str]:
    id_regex = compile(r"\[]\(q://(.+?)\) {2}")
    id_match = id_regex.match(line)
    if id_match:
        line = line.removeprefix(id_match.group(0))
        buffer_node_id = BufferNodeId(id_match.group(1))
        node_id = buffer_to_node_id(buffer_node_id)
    else:
        node_id = get_node_id()
    return node_id, line


def content_lines_to_buffer_lines(content_lines: list[str], node_id: NodeId, level: int, expanded: bool,
                                  ordered: bool) -> tuple[
    BufferNodeId, list[str]]:
    buffer_id = node_to_buffer_id(node_id)
    if level == 0:
        buffer_lines = content_lines
    else:
        offset = 3 if ordered else 2
        space_count = LEVEL_SPACES * (level - 1) + offset
        space_prefix = ' ' * space_count
        buffer_lines = [
            space_prefix[
            :-offset] + f"{'1.' if ordered else (EXPANDED_BULLET if expanded else COLLAPSED_BULLET)} [](q://{buffer_id})  " +
            content_lines[
                0]]
        for idx, line in enumerate(content_lines[1:]):
            buffer_lines.append(space_prefix + line)
    return buffer_id, buffer_lines


def previous_sibling_node_line_range(list_item_ast: SyntaxTreeNode, node_id: NodeId) -> tuple[int, int]:
    while True:
        assert list_item_ast.previous_sibling, (node_id, list_item_ast.map)
        if list_item_ast.previous_sibling.meta[NODE_ID_ATTR] == node_id:
            node_loc = list_item_ast.previous_sibling.map
            break
        list_item_ast = list_item_ast.previous_sibling
    return node_loc


def raise_if_duplicate_sibling(list_item_ast: SyntaxTreeNode, node_id: NodeId, tree: Tree) -> None:
    if node_id in tree:
        sibling_line_range = previous_sibling_node_line_range(list_item_ast, node_id)
        raise DuplicateException(node_id, (list_item_ast.map, sibling_line_range))


def get_ast_sub_lists(list_item_ast: SyntaxTreeNode) -> list[
    SyntaxTreeNode]:  # TODO: Merge two loops, line range updation here instead of process list asts?
    child_list_asts = []
    if list_item_ast.children:
        cur_child_list_ast = list_item_ast.children[-1]
        while cur_child_list_ast.type.endswith("_list"):
            child_list_asts.append(cur_child_list_ast)
            cur_child_list_ast = cur_child_list_ast.previous_sibling
            if not cur_child_list_ast or cur_child_list_ast is list_item_ast.children[0]:
                break
    child_list_asts.reverse()

    last_type = None
    merged_child_asts: list[SyntaxTreeNode] = []
    for cur_child_list_ast in child_list_asts:
        cur_type = cur_child_list_ast.type
        if cur_type == last_type:
            last_child_list_ast = merged_child_asts[-1]
            last_child_list_ast.children.extend(cur_child_list_ast.children)

            token_obj = last_child_list_ast.token or last_child_list_ast.nester_tokens.opening
            token_obj.map = last_child_list_ast.map[0], cur_child_list_ast.map[1]

            for child_ast in cur_child_list_ast.children:
                child_ast.parent = last_child_list_ast
        else:
            merged_child_asts.append(cur_child_list_ast)
        last_type = cur_type

    return merged_child_asts


def preserve_expand_consider_sub_tree(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: Tree,
                                      ledger: Ledger):
    bullet = list_item_ast.markup

    parent_ast = list_item_ast.previous_sibling if (
            list_item_ast.parent.type == 'ordered_list' and list_item_ast.previous_sibling) else list_item_ast.parent.parent
    parent_node_id = parent_ast.meta[NODE_ID_ATTR]

    not_new = parent_node_id in ledger and node_id in ledger[parent_node_id].children_ids

    if not_new:
        consider_sub_tree = bullet not in (COLLAPSED_BULLET, TO_EXPAND_BULLET)
    else:
        children = get_key_val(node_id, states.cursors.children)

        if children is None:
            consider_sub_tree = True
        else:
            if sub_list_tree and sub_list_tree.keys() ^ children:
                raise CloneChildrenException(node_id, list_item_ast.map)
            consider_sub_tree = False

    expand = bullet == TO_EXPAND_BULLET or (bullet != COLLAPSED_BULLET and sub_list_tree)

    return expand, consider_sub_tree


def create_root_if_new(root_id: NodeId) -> None:
    cursors = states.cursors
    for cursor, val in ((cursors.content, ['']), (cursors.children, []), (cursors.views, {})):
        if get_key_val(root_id, cursor) is None:
            put_key_val(root_id, val, cursor)
    for cursor in (cursors.unsynced_content, cursors.unsynced_children, cursors.unsynced_views):
        put_key_val(root_id, True, cursor)