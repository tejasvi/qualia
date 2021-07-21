from itertools import zip_longest
from typing import Union

from markdown_it.tree import SyntaxTreeNode

from qualia import BufferNodeId
from qualia.models import NodeId, View, ProcessState, NODE_ID_ATTR
from qualia.states import ledger
from qualia.utils import get_md_ast, get_node_id, conflict, split_id_from_line, raise_if_duplicate_sibling, \
    get_ast_sub_lists, should_process_children


class Process:
    def __init__(self) -> None:
        self._lines = None
        self._changes = None

    def process_lines(self, lines: list[str], root_id: NodeId) -> tuple[View, ProcessState]:
        self._changes = ProcessState()
        self._lines = lines

        self._lines[0] = f"[](q://{BufferNodeId(root_id)})  " + self._lines[0]

        root_tree: dict[NodeId, dict] = {}  # {root_id: {child_1: {..}, child_2: {..}, ..}}

        root_ast = get_md_ast(lines)
        self._process_list_item_ast(root_ast, root_tree)

        data = root_tree.popitem()
        root_view = View(*data)
        return root_view, self._changes

    def _process_list_item_ast(self, list_item_ast: SyntaxTreeNode, tree: dict[NodeId, dict]) -> dict[NodeId, dict]:
        root_ast = list_item_ast.type == 'root'
        content_start_line_num = list_item_ast.map[0]
        content_indent = 0 if root_ast else self._lines[content_start_line_num].index(
            list_item_ast.markup) + 2
        first_line = self._lines[content_start_line_num][content_indent:]
        node_id, id_line = split_id_from_line(first_line)
        if node_id is None:
            node_id = get_node_id()
        list_item_ast.meta[NODE_ID_ATTR] = node_id

        sub_lists = get_ast_sub_lists(list_item_ast)

        content_end_line_num = sub_lists[0].map[0] if sub_lists else list_item_ast.map[1]

        content_lines = [id_line] + [
            line.removeprefix(" " * content_indent)
            for line in self._lines[content_start_line_num + 1: content_end_line_num]
        ]

        sub_list_tree = self._process_list_item_asts(sub_lists)

        raise_if_duplicate_sibling(list_item_ast, node_id, sub_list_tree, tree)

        expanded = root_ast or should_process_children(list_item_ast, node_id, sub_list_tree)
        tree[node_id] = sub_list_tree if expanded else {}
        self._process_node(node_id, content_lines, set(sub_list_tree) if expanded else None)
        return sub_list_tree

    def _process_list_item_asts(self, list_item_asts: list[SyntaxTreeNode]) -> dict[NodeId, dict]:
        sub_list_tree = {}
        for list_item_ast in list_item_asts:
            ordered_list = list_item_ast.type == 'ordered_list'
            cur_sub_list_tree = sub_list_tree
            for child_list_item_ast, next_child_list_item_ast in zip_longest(list_item_ast.children,
                                                                             list_item_ast.children[1:],
                                                                             fillvalue=None):
                (child_list_item_ast.token or child_list_item_ast.nester_tokens.opening).map = child_list_item_ast.map[
                                                                                                   0], \
                                                                                               next_child_list_item_ast.map[
                                                                                                   0] if next_child_list_item_ast else \
                                                                                                   child_list_item_ast.parent.map[
                                                                                                       1]

                child_sub_list_tree = self._process_list_item_ast(child_list_item_ast, cur_sub_list_tree)
                if ordered_list:
                    cur_sub_list_tree = child_sub_list_tree
        return sub_list_tree

    def _process_node(self, node_id: NodeId, content_lines: list[str], children_ids: Union[None, set[NodeId]]):
        if node_id not in ledger:
            self._changes.changed_content_map[node_id] = content_lines
            if children_ids is not None:
                self._changes.changed_children_map[node_id] = children_ids
            return

        # Assuming real-time update else suppose user changes a node then scrolls to portion of
        # buffer containing the node's clone but with stale content. Now user writes the buffer
        # manually expecting the visible node to stay the same but it changes. Though the incoming
        # change is similar to the change coming from external syncing source.

        content_changed = ledger[node_id].content_lines != content_lines
        if content_changed:
            if node_id in self._changes.changed_content_map:
                self._changes.changed_content_map[node_id] = conflict(content_lines,
                                                                      self._changes.changed_content_map[node_id], True)
            else:
                self._changes.changed_content_map[node_id] = content_lines

        children_changed = children_ids is not None and children_ids != ledger[node_id].children_ids
        if children_changed:
            if node_id in self._changes.changed_children_map:
                self._changes.changed_children_map[node_id].update(children_ids)
            else:
                self._changes.changed_children_map[node_id] = children_ids
