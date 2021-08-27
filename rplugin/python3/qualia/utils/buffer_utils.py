from re import compile
from typing import Union, cast

from lmdb import Cursor
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode

from qualia.config import _COLLAPSED_BULLET, _TO_EXPAND_BULLET
from qualia.models import NODE_ID_ATTR, Tree, NodeId, BufferNodeId, DuplicateNodeException, LastSeen, \
    UncertainNodeChildrenException
from qualia.utils.common_utils import removeprefix, get_time_uuid, get_key_val

_md_parser = MarkdownIt("zero", {"maxNesting": float('inf')}).enable(
    ["link", "list", "code", "fence", "html_block"]).parse


def get_md_ast(content_lines: list[str]) -> SyntaxTreeNode:
    root_ast = SyntaxTreeNode(_md_parser('\n'.join(content_lines)))
    root_ast.token = Token(meta={}, map=[0, len(content_lines)], nesting=0, tag="", type="root")
    return root_ast


def buffer_to_node_id(buffer_id: BufferNodeId, buffer_to_node_id_cur: Cursor) -> Union[None, NodeId]:
    # return NodeId(buffer_id)
    node_id = get_key_val(buffer_id, buffer_to_node_id_cur)
    assert node_id is not None
    return cast(NodeId, node_id)


def get_id_line(line: str, buffer_to_node_id_cur: Cursor) -> tuple[NodeId, str]:
    id_regex = compile(r"\[]\((.{1,2})\) {0,2}")
    id_match = id_regex.match(line)
    if id_match:
        line = removeprefix(line, id_match.group(0))
        buffer_node_id = BufferNodeId(id_match.group(1))
        node_id = buffer_to_node_id(buffer_node_id, buffer_to_node_id_cur)
    else:
        node_id = get_node_id()
    return node_id, line


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
        raise DuplicateNodeException(node_id, (list_item_ast.map, sibling_line_range))


def get_ast_sub_lists(list_item_ast: SyntaxTreeNode) -> list[
    SyntaxTreeNode]:  # TODO: Merge two loops, line range updation here instead of process list asts?
    # FIX: Can't call twice -> reduce to one node
    child_list_asts = []
    if list_item_ast.children:
        cur_child_list_ast = list_item_ast.children[-1]
        while cur_child_list_ast.type.endswith("_list"):
            child_list_asts.append(cur_child_list_ast)
            cur_child_list_ast = cur_child_list_ast.previous_sibling
            if not cur_child_list_ast or cur_child_list_ast is list_item_ast.children[0]:
                break
    child_list_asts.reverse()

    merged_child_asts = merge_sibling_lists_ignore_bullets(child_list_asts)

    return merged_child_asts


def merge_sibling_lists_ignore_bullets(child_list_asts: list[SyntaxTreeNode]) -> list[SyntaxTreeNode]:
    last_type = None
    merged_child_asts: list[SyntaxTreeNode] = []
    for cur_child_list_ast in child_list_asts:
        cur_type = cur_child_list_ast.type
        if cur_type == last_type or (cur_type.endswith("_list") and last_type and last_type.endswith("_list")):
            last_child_list_ast = merged_child_asts[-1]
            last_nester_tokens = last_child_list_ast.nester_tokens

            token_obj = last_child_list_ast.token or last_nester_tokens.opening
            token_obj.map = last_child_list_ast.map[0], cur_child_list_ast.map[1]

            if cur_type == 'ordered_list' and last_type == "bullet_list":
                copy_list_ast_type(last_child_list_ast, cur_child_list_ast)

            last_child_list_ast.children.extend(cur_child_list_ast.children)

            for child_ast in cur_child_list_ast.children:
                child_ast.parent = last_child_list_ast

        else:
            merged_child_asts.append(cur_child_list_ast)
        last_type = cur_type
    return merged_child_asts


def copy_list_ast_type(target_list_ast: SyntaxTreeNode, source_list_ast: SyntaxTreeNode) -> None:
    # skip since markup used for finding indent
    # list_markup = source_list_ast.children[0].nester_tokens.closing.markup
    # for child_ast in target_list_ast.children:
    #     child_ast.nester_tokens.closing.markup = child_ast.nester_tokens.opening.markup = list_markup

    target_list_ast.nester_tokens.opening.type = source_list_ast.nester_tokens.opening.type
    target_list_ast.nester_tokens.closing.type = source_list_ast.nester_tokens.closing.type

    target_list_ast.nester_tokens.opening.tag = source_list_ast.nester_tokens.opening.tag
    target_list_ast.nester_tokens.closing.tag = source_list_ast.nester_tokens.closing.tag


def preserve_expand_consider_sub_tree(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: Tree,
                                      last_seen: LastSeen):
    bullet = list_item_ast.markup

    parent_ast = list_item_ast.previous_sibling if (list_item_ast.parent.type == 'ordered_list'
                                                    and list_item_ast.previous_sibling) else list_item_ast.parent.parent
    parent_node_id = parent_ast.meta[NODE_ID_ATTR]

    not_new = parent_node_id in last_seen and node_id in last_seen[parent_node_id].children_ids

    if not_new:
        consider_sub_tree = bullet not in (_COLLAPSED_BULLET, _TO_EXPAND_BULLET)
    else:
        if sub_list_tree:
            if node_id in last_seen and sub_list_tree.keys() ^ last_seen[node_id].children_ids:
                raise UncertainNodeChildrenException(node_id, list_item_ast.map)
            else:
                consider_sub_tree = True
        else:
            consider_sub_tree = False

    expand = bullet == _TO_EXPAND_BULLET or (bullet != _COLLAPSED_BULLET and sub_list_tree)

    return expand, consider_sub_tree


def get_node_id() -> NodeId:
    while True:
        node_id = get_time_uuid()
        if ")" not in node_id:
            break
    return node_id
