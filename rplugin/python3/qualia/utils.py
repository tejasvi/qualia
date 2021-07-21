from base64 import urlsafe_b64encode
from dataclasses import dataclass
from hashlib import sha256
from json import loads, dumps
from re import compile
from secrets import token_urlsafe
from time import time_ns
from typing import Callable, Tuple, Union
from uuid import uuid4

import lmdb
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode
from pynvim import Nvim

from qualia import DuplicateException, CloneException
from qualia.config import DB_FOLDER, LEVEL_SPACES, EXPANDED_BULLET, COLLAPSED_BULLET
from qualia.models import NodeId, JSONType, BufferNodeId, NODE_ID_ATTR
from qualia.states import ledger

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


def get_buffer_id(node_id: NodeId) -> BufferNodeId:
    return BufferNodeId(node_id)
    # base65536 doesn't output brackets https://qntm.org/safe
    # return base65536.encode(node_id.encode())


@dataclass
class Cursors:
    content: lmdb.Cursor
    children: lmdb.Cursor
    views: lmdb.Cursor


class Database:
    def __enter__(self) -> Cursors:
        self.env = env = lmdb.open(DB_FOLDER, max_dbs=3)
        self.txn = env.begin(write=True)
        return Cursors(**{db_name: self.sub_db(db_name) for db_name in ("content", "children", "views")})

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


def split_id_from_line(line: str) -> Tuple[Union[NodeId, None], str]:
    node_id = None
    id_regex = compile(r"\[]\(q://(.+?)\) {2}")
    id_match = id_regex.match(line)
    if id_match:
        buffer_node_id = BufferNodeId(id_match.group(1))
        if buffer_node_id in ledger.buffer_node_id_map:
            node_id = ledger.buffer_node_id_map[buffer_node_id]
            line = line.removeprefix(id_match.group(0))
    return node_id, line


def content_lines_to_buffer_lines(content_lines: list[str], node_id: NodeId, level: int, expanded: bool) -> tuple[
    BufferNodeId, list[str]]:
    buffer_id = get_buffer_id(node_id)
    if level == 0:
        buffer_lines = content_lines
    else:
        space_count = LEVEL_SPACES * (level - 1) + 2
        space_prefix = ' ' * space_count
        buffer_lines = [
            space_prefix[:-2] + f"{EXPANDED_BULLET if expanded else COLLAPSED_BULLET} [](q://{buffer_id})  " +
            content_lines[
                0]]
        for idx, line in enumerate(content_lines[1:]):
            buffer_lines.append(space_prefix + line)
    return buffer_id, buffer_lines


def get_previous_sibling_node_loc(list_item_ast: SyntaxTreeNode, node_id: NodeId) -> tuple[int, int]:
    while True:
        assert list_item_ast
        if list_item_ast.previous_sibling.meta[NODE_ID_ATTR] == node_id:
            node_loc = list_item_ast.previous_sibling.map
            break
        list_item_ast = list_item_ast.previous_sibling
    return node_loc


def raise_if_duplicate_sibling(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: dict[NodeId, dict],
                               tree: dict[NodeId, dict]) -> None:
    if node_id in tree:
        clone_node_loc = list_item_ast.map
        other_node_loc = None
        if sub_list_tree and tree[node_id]:
            other_node_loc = get_previous_sibling_node_loc(list_item_ast, node_id)
        elif sub_list_tree and not tree[node_id]:
            clone_node_loc = get_previous_sibling_node_loc(list_item_ast, node_id)
        raise (DuplicateException if other_node_loc else CloneException)(node_id, clone_node_loc, other_node_loc)


def get_ast_sub_lists(list_item_ast: SyntaxTreeNode) -> list[SyntaxTreeNode]:
    sub_lists = []
    if list_item_ast.children:
        cur_child_ast = list_item_ast.children[-1]
        while cur_child_ast.type.endswith("_list"):
            sub_lists.append(cur_child_ast)
            cur_child_ast = cur_child_ast.previous_sibling
            if not cur_child_ast or cur_child_ast is list_item_ast.children[0]:
                break
    sub_lists.reverse()
    return sub_lists


def should_process_children(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: dict[NodeId, dict]):
    parent_node_id = list_item_ast.parent.parent.meta[NODE_ID_ATTR]
    return list_item_ast.markup == EXPANDED_BULLET and (
            (parent_node_id in ledger and node_id in ledger[parent_node_id].children_ids) or sub_list_tree)
