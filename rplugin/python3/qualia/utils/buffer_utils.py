from __future__ import annotations

import binascii
from functools import cache
from re import compile
from typing import cast, Optional, TYPE_CHECKING, Callable, Sequence
from uuid import UUID

from qualia.config import _COLLAPSED_BULLET, _TO_EXPAND_BULLET, _SHORT_BUFFER_ID
from qualia.models import NODE_ID_ATTR, Tree, NodeId, ShortId, DuplicateNodeException, LastSync, \
    UncertainNodeChildrenException, AstMap, Li, KeyNotFoundError, MinimalDb
from qualia.utils.common_utils import get_time_uuid, buffer_id_decoder, removeprefix, InvalidBufferNodeIdError

if TYPE_CHECKING:
    from markdown_it.tree import SyntaxTreeNode
    from markdown_it.token import Token


@cache
def _md_parser() -> Callable[[str], Sequence[Token]]:
    from markdown_it import MarkdownIt
    return MarkdownIt("zero", {"maxNesting": float('inf')}).enable(
        ["link", "list", "code", "fence", "html_block"]).parse


def get_md_ast(content_lines: Li) -> SyntaxTreeNode:
    from markdown_it.tree import SyntaxTreeNode
    from markdown_it.token import Token
    root_ast = SyntaxTreeNode(_md_parser()('\n'.join(content_lines)))
    root_ast.token = Token(meta={}, map=[0, len(content_lines)], nesting=0, tag="", type="root")
    return root_ast


def buffer_to_node_id(buffer_node_id: ShortId, db: MinimalDb) -> NodeId:
    buffer_id_bytes = buffer_id_decoder(buffer_node_id)
    try:
        node_id = db.buffer_id_bytes_to_node_id(buffer_id_bytes)
    except (KeyNotFoundError, binascii.Error):
        raise InvalidBufferNodeIdError(buffer_node_id)
    return node_id


def get_id_line(line: str, db: MinimalDb) -> tuple[NodeId, str]:
    id_regex = compile(r"\[]\(.(.+?)\) {0,2}")
    id_match = id_regex.match(line)
    if id_match:
        line = removeprefix(line, id_match.group(0))
        buffer_node_id = ShortId(id_match.group(1))
        if _SHORT_BUFFER_ID:
            node_id = buffer_to_node_id(buffer_node_id, db)
        else:
            UUID(buffer_node_id)
            node_id = cast(NodeId, buffer_node_id)
    else:
        node_id = get_time_uuid()
    return node_id, line


def previous_sibling_node_line_range(list_item_ast: SyntaxTreeNode, node_id: NodeId) -> AstMap:
    previous_sibling = list_item_ast.previous_sibling
    assert list_item_ast.parent and previous_sibling
    ordered_item = list_item_ast.parent.type == 'ordered_list'
    node_loc = (_ordered_locate_previous_sibling if ordered_item else
                _unordered_locate_previous_sibling)(previous_sibling, node_id)
    assert node_loc, f"{node_id=} not found in {list_item_ast=} at {list_item_ast.map=}. {ordered_item=}"
    return node_loc


def _ordered_locate_previous_sibling(ordered_list_item_ast: SyntaxTreeNode, node_id: NodeId) -> AstMap:
    for item_sub_ast in ordered_list_item_ast.children:
        if item_sub_ast.type.endswith("_list"):
            item_sub_list_ast = item_sub_ast
            if item_sub_list_ast.type == "ordered_list" and item_sub_list_ast.children:
                first_ordered_list_item_ast = item_sub_list_ast.children[0]
                assert first_ordered_list_item_ast.meta[NODE_ID_ATTR] == node_id
                assert first_ordered_list_item_ast.map
                return first_ordered_list_item_ast.map
            else:
                assert item_sub_list_ast.children
                cur_list_item_ast = item_sub_list_ast.children[-1]
                node_loc = _unordered_locate_previous_sibling(cur_list_item_ast, node_id)
                if node_loc is not None:
                    return node_loc
    raise Exception(f"{node_id=} not found in {ordered_list_item_ast=} at {ordered_list_item_ast.map=}")


def _unordered_locate_previous_sibling(begin_unordered_list_item_ast: SyntaxTreeNode, node_id: NodeId) -> Optional[
    AstMap]:
    cur_list_item_ast: Optional[SyntaxTreeNode] = begin_unordered_list_item_ast
    while True:
        assert cur_list_item_ast
        if cur_list_item_ast.meta[NODE_ID_ATTR] == node_id:
            return cur_list_item_ast.map
        cur_list_item_ast = cur_list_item_ast.previous_sibling
        if cur_list_item_ast is None:
            return None


def raise_if_duplicate_sibling(list_item_ast: SyntaxTreeNode, node_id: NodeId, tree: Tree) -> None:
    if node_id in tree:
        sibling_line_range = previous_sibling_node_line_range(list_item_ast, node_id)
        raise DuplicateNodeException(node_id, (cast(AstMap, list_item_ast.map), sibling_line_range))


def get_ast_sub_lists(list_item_ast: SyntaxTreeNode) -> list[
    SyntaxTreeNode]:  # TODO: Merge two loops, line range updation here instead of process list asts?
    # Won't work if called again using same ast TODO: reduce to one node
    descendant_list_asts = []
    if list_item_ast.children:
        cur_descendant_list_ast = list_item_ast.children[-1]
        while cur_descendant_list_ast.type.endswith("_list"):
            descendant_list_asts.append(cur_descendant_list_ast)
            if cur_descendant_list_ast.previous_sibling is None:
                break
            cur_descendant_list_ast = cur_descendant_list_ast.previous_sibling
            if cur_descendant_list_ast is list_item_ast.children[0]:
                break
    descendant_list_asts.reverse()

    merged_descendant_asts = merge_every_type_sibling_lists(descendant_list_asts)

    return merged_descendant_asts


def merge_every_type_sibling_lists(descendant_list_asts: list[SyntaxTreeNode]) -> list[SyntaxTreeNode]:
    last_type = None
    merged_descendant_asts: list[SyntaxTreeNode] = []
    for cur_descendant_list_ast in descendant_list_asts:
        cur_type = cur_descendant_list_ast.type
        if cur_type == last_type or (cur_type.endswith("_list") and last_type and last_type.endswith("_list")):
            # Always gives true since currently merging all list types into one (ordered has priority).
            # Therefore len(merged_descendant_asts) == 1
            last_descendant_list_ast = merged_descendant_asts[-1]
            last_nester_tokens = last_descendant_list_ast.nester_tokens
            assert last_nester_tokens

            token_obj = last_descendant_list_ast.token or last_nester_tokens.opening
            token_obj.map = [cast(AstMap, last_descendant_list_ast.map)[0],
                             cast(AstMap, cur_descendant_list_ast.map)[1]]

            if cur_type == 'ordered_list' and last_type == "bullet_list":
                copy_list_ast_type(last_descendant_list_ast, cur_descendant_list_ast)

            last_descendant_list_ast.children.extend(cur_descendant_list_ast.children)

            for descendant_ast in cur_descendant_list_ast.children:
                descendant_ast.parent = last_descendant_list_ast

        else:
            merged_descendant_asts.append(cur_descendant_list_ast)
        last_type = cur_type
    return merged_descendant_asts


def copy_list_ast_type(target_list_ast: SyntaxTreeNode, source_list_ast: SyntaxTreeNode) -> None:
    # skip copying markup since used for finding correct indent (- vs 1.)
    # list_markup = source_list_ast.children[0].nester_tokens.closing.markup
    # for descendant_ast in target_list_ast.children:
    #     descendant_ast.nester_tokens.closing.markup = descendant_ast.nester_tokens.opening.markup = list_markup

    # _NesterTokens.opening, _NesterTokens.closing
    # Token.tag, Token.type
    for state in ("opening", "closing"):
        for attr in ("tag", "type"):
            setattr(getattr(target_list_ast.nester_tokens, state), attr,
                    getattr(getattr(source_list_ast.nester_tokens, state), attr))


def preserve_expand_consider_sub_tree(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: Tree,
                                      last_sync: LastSync):
    bullet = list_item_ast.markup

    assert list_item_ast.parent
    ancestor_ast = list_item_ast.previous_sibling if (
            list_item_ast.parent.type == 'ordered_list' and list_item_ast.previous_sibling
    ) else list_item_ast.parent.parent
    assert ancestor_ast is not None
    ancestor_node_id = ancestor_ast.meta[NODE_ID_ATTR]

    not_new_descendant = ancestor_node_id in last_sync and node_id in last_sync[ancestor_node_id].descendants_ids

    if not_new_descendant:
        consider_sub_tree = bullet not in (_COLLAPSED_BULLET, _TO_EXPAND_BULLET)
    else:
        if sub_list_tree:
            if node_id in last_sync and last_sync[node_id].descendants_ids.symmetric_difference(sub_list_tree.keys()):
                raise UncertainNodeChildrenException(node_id, cast(AstMap, list_item_ast.map))
            else:
                consider_sub_tree = True
        else:
            consider_sub_tree = False

    expand = bullet != _COLLAPSED_BULLET
    # expand = bullet == _TO_EXPAND_BULLET or (bullet != _COLLAPSED_BULLET and sub_list_tree)  # Why?

    return expand, consider_sub_tree
