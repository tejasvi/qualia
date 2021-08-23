from __future__ import annotations

from itertools import zip_longest
from typing import Union, Iterator

from markdown_it.tree import SyntaxTreeNode
from orderedset import OrderedSet

from qualia.models import NodeId, View, ProcessState, NODE_ID_ATTR, Tree, LastSeen, BufferNodeId
from qualia.utils import get_md_ast, conflict, get_id_line, raise_if_duplicate_sibling, \
    get_ast_sub_lists, preserve_expand_consider_sub_tree, removeprefix


class Process:
    def __init__(self) -> None:
        self._lines = None
        self._changes = None

    def process_lines(self, lines: list[str], main_id: NodeId, last_seen: LastSeen) -> tuple[
        View, ProcessState]:
        self._changes = ProcessState()
        self._lines = lines

        self._lines[0] = f"[](q://{BufferNodeId(main_id)})  " + self._lines[0]

        buffer_tree: Tree = {}  # {node_id: {child_1: {..}, child_2: {..}, ..}}

        buffer_ast = get_md_ast(lines)
        self._process_list_item_ast(buffer_ast, buffer_tree, iter([]), last_seen)

        data = buffer_tree.popitem()
        root_view = View(*data)
        return root_view, self._changes

    def _process_list_item_ast(self, list_item_ast: SyntaxTreeNode, tree: Tree,
                               ordered_child_asts: Iterator[SyntaxTreeNode], last_seen: LastSeen):
        is_buffer_ast = list_item_ast.type == 'root'
        content_start_line_num = list_item_ast.map[0]
        content_indent = 0 if is_buffer_ast else self._lines[content_start_line_num].index(
            list_item_ast.markup) + 2
        first_line = self._lines[content_start_line_num][content_indent:]
        node_id, id_line = get_id_line(first_line)
        list_item_ast.meta[NODE_ID_ATTR] = node_id

        sub_lists = get_ast_sub_lists(list_item_ast)
        sub_list_tree = self._process_list_item_asts(sub_lists, last_seen)
        try:
            first_ordered_child_ast = next(ordered_child_asts)
            self._process_list_item_ast(first_ordered_child_ast, sub_list_tree, ordered_child_asts, last_seen)
        except StopIteration:
            pass

        content_end_line_num = sub_lists[0].map[0] if sub_lists else list_item_ast.map[1]

        raise_if_duplicate_sibling(list_item_ast, node_id, tree)

        expand, consider_sub_list_tree = (True, True) if is_buffer_ast else preserve_expand_consider_sub_tree(
            list_item_ast, node_id, sub_list_tree, last_seen)
        tree[node_id] = sub_list_tree if expand else None

        content_lines = [id_line] + [
            removeprefix(line, " " * content_indent)
            for line in self._lines[content_start_line_num + 1: content_end_line_num]
        ]

        self._process_node(node_id, content_lines, OrderedSet(sub_list_tree) if consider_sub_list_tree else None,
                           last_seen)

    def _process_list_item_asts(self, list_item_asts: list[SyntaxTreeNode], last_seen: LastSeen) -> Tree:
        sub_list_tree = {}
        for list_item_ast, list_end_line in zip_longest(list_item_asts, (ast.map[0] for ast in list_item_asts[1:]),
                                                        fillvalue=list_item_asts and list_item_asts[0].parent.map[1]):
            children_asts = list_item_ast.children
            if not children_asts:
                continue
            later_children_asts = children_asts[1:]

            ordered_list = list_item_ast.type == 'ordered_list'

            for child_list_item_ast, item_end_line in zip_longest(children_asts,
                                                                  (ast.map[0] for ast in later_children_asts),
                                                                  fillvalue=list_end_line):
                token_obj = child_list_item_ast.token or child_list_item_ast.nester_tokens.opening
                token_obj.map = child_list_item_ast.map[0], item_end_line

                if not ordered_list:
                    self._process_list_item_ast(child_list_item_ast, sub_list_tree, iter([]), last_seen)

            if ordered_list:
                self._process_list_item_ast(children_asts[0], sub_list_tree, iter(later_children_asts), last_seen)
        return sub_list_tree

    def _process_node(self, node_id: NodeId, content_lines: list[str], children_ids: Union[None, OrderedSet],
                      last_seen: LastSeen):
        if node_id not in last_seen:
            self._changes.changed_content_map[node_id] = content_lines
            if children_ids is not None:
                self._changes.changed_children_map[node_id] = children_ids
            return

        # Assuming real-time update else suppose user changes a node then scrolls to portion of
        # buffer containing the node's clone but with stale content. Now user writes the buffer
        # manually expecting the visible node to stay the same but it changes. Though the incoming
        # change is similar to the change coming from external syncing source.

        content_changed = last_seen[node_id].content_lines != content_lines
        if content_changed:
            if node_id in self._changes.changed_content_map:
                self._changes.changed_content_map[node_id] = conflict(content_lines,
                                                                      self._changes.changed_content_map[node_id])
            else:
                self._changes.changed_content_map[node_id] = content_lines

        children_changed = children_ids is not None and (children_ids ^ last_seen[node_id].children_ids)
        if children_changed:
            if node_id in self._changes.changed_children_map:
                self._changes.changed_children_map[node_id].update(children_ids)
            else:
                self._changes.changed_children_map[node_id] = children_ids
