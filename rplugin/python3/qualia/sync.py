from __future__ import annotations

from itertools import zip_longest
from threading import Event
from time import time
from typing import cast, Union, TYPE_CHECKING, Iterator

from orderedset import OrderedSet

from qualia.database import Database
from qualia.models import View, NodeId, LastSync, Li, ProcessState, Tree, NODE_ID_ATTR, AstMap, MinimalDb
from qualia.services.realtime import Realtime
from qualia.services.utils.realtime_utils import sync_with_realtime_db
from qualia.utils.buffer_utils import get_md_ast, get_id_line, get_ast_sub_lists, raise_if_duplicate_sibling, \
    preserve_expand_consider_sub_tree
from qualia.utils.common_utils import conflict, removeprefix
from qualia.utils.render_utils import buffer_node_tracker
from qualia.utils.sync_utils import sync_with_db

if TYPE_CHECKING:
    from markdown_it.tree import SyntaxTreeNode


def sync_buffer(buffer_lines: Li, main_id: NodeId, last_sync: LastSync, db: Database,
                transposed: bool, realtime_session: Realtime, git_sync_event: Event) -> View:
    if main_id in last_sync:
        main_view, changes = ParseProcess().process_lines(buffer_lines, main_id, last_sync, db, transposed)
        realtime_data = sync_with_db(main_view, changes, last_sync, db, transposed, realtime_session.others_online)
        sync_with_realtime_db(realtime_data, realtime_session)
        if changes and realtime_session.last_broadcast_recieve_time < time() - 15:
            git_sync_event.set()
    else:
        main_view = db.get_node_view(main_id, transposed)
    return main_view


class ParseProcess:
    _lines: Li
    _changes: ProcessState
    db: MinimalDb

    def __init__(self) -> None:
        pass

    def process_lines(self, lines: Li, main_id: NodeId, last_sync: LastSync, db: MinimalDb, transposed) -> tuple[
        View, ProcessState]:
        if not lines:
            lines = cast(Li, [''])
        self.db = db
        self._changes = ProcessState()
        self._lines = lines

        self._lines[0] = buffer_node_tracker(main_id, transposed, db) + self._lines[0]

        buffer_tree: Tree = {}  # {node_id: {descendant_1: {..}, descendant_2: {..}, ..}}

        buffer_ast = get_md_ast(self._lines)
        self._process_list_item_ast(buffer_ast, buffer_tree, iter([]), last_sync)

        data = buffer_tree.popitem()
        root_view = View(*data, transposed)
        return root_view, self._changes

    def _process_list_item_ast(self, list_item_ast, tree, ordered_descendant_asts, last_sync):
        # type:(SyntaxTreeNode, Tree, Iterator[SyntaxTreeNode], LastSync)->None
        is_buffer_ast = list_item_ast.type == 'root'
        assert list_item_ast.map
        content_start_line_num = list_item_ast.map[0]
        content_indent = 0 if is_buffer_ast else self._lines[content_start_line_num].index(
            list_item_ast.markup) + 2
        first_line = self._lines[content_start_line_num][content_indent:]
        node_id, id_line = get_id_line(first_line, self.db)
        list_item_ast.meta[NODE_ID_ATTR] = node_id

        sub_lists = get_ast_sub_lists(list_item_ast)
        sub_list_tree = self._process_list_item_asts(sub_lists, last_sync)
        try:
            first_ordered_descendant_ast = next(ordered_descendant_asts)
            self._process_list_item_ast(first_ordered_descendant_ast, sub_list_tree, ordered_descendant_asts, last_sync)
        except StopIteration:
            pass

        content_end_line_num = cast(AstMap, sub_lists[0].map)[0] if sub_lists else list_item_ast.map[1]

        raise_if_duplicate_sibling(list_item_ast, node_id, tree)

        expand, consider_sub_list_tree = (True, True) if is_buffer_ast else preserve_expand_consider_sub_tree(
            list_item_ast, node_id, sub_list_tree, last_sync)
        tree[node_id] = sub_list_tree if expand else None

        content_lines = cast(Li, [id_line] + [
            removeprefix(line, " " * content_indent)
            for line in self._lines[content_start_line_num + 1: content_end_line_num]
        ])

        self._process_node(node_id, content_lines, OrderedSet(sub_list_tree) if consider_sub_list_tree else None,
                           last_sync)

    def _process_list_item_asts(self, list_item_asts, last_sync):
        # type:(list[SyntaxTreeNode], LastSync) -> Tree
        sub_list_tree: Tree = {}
        if not list_item_asts:
            return sub_list_tree
        parent_list_ast = list_item_asts[0].parent
        assert parent_list_ast
        ast_parent_end_line = cast(AstMap, parent_list_ast.map)[1]
        for list_item_ast, list_end_line in zip_longest(list_item_asts,
                                                        (cast(AstMap, ast.map)[0] for ast in list_item_asts[1:]),
                                                        fillvalue=ast_parent_end_line):
            descendant_asts = list_item_ast.children
            if not descendant_asts:
                continue
            later_descendant_asts = descendant_asts[1:]

            ordered_list = list_item_ast.type == 'ordered_list'

            for descendant_list_item_ast, item_end_line in zip_longest(descendant_asts,
                                                                       (ast.map[0] for ast in later_descendant_asts),
                                                                       fillvalue=list_end_line):
                token_obj = descendant_list_item_ast.token or descendant_list_item_ast.nester_tokens.opening
                token_obj.map = descendant_list_item_ast.map[0], item_end_line

                if not ordered_list:
                    self._process_list_item_ast(descendant_list_item_ast, sub_list_tree, iter([]), last_sync)

            if ordered_list:
                self._process_list_item_ast(descendant_asts[0], sub_list_tree, iter(later_descendant_asts), last_sync)
        return sub_list_tree

    def _process_node(self, node_id: NodeId, content_lines: Li, descendant_ids: Union[None, OrderedSet],
                      last_sync: LastSync):
        if node_id not in last_sync:
            self._changes.changed_content_map[node_id] = content_lines
            if descendant_ids is not None:
                self._changes.changed_descendants_map[node_id] = descendant_ids
            return

        # Assuming real-time update else suppose user changes a node then scrolls to portion of
        # buffer containing the node's clone but with stale content. Now user writes the buffer
        # manually expecting the visible node to stay the same but it changes. Though the incoming
        # change is similar to the change coming from external syncing source.

        content_changed = last_sync[node_id].content_lines != content_lines
        if content_changed:
            if node_id in self._changes.changed_content_map:
                self._changes.changed_content_map[node_id] = conflict(content_lines,
                                                                      self._changes.changed_content_map[node_id])
            else:
                self._changes.changed_content_map[node_id] = content_lines

        descendant_changed = descendant_ids is not None and (
            descendant_ids.symmetric_difference(last_sync[node_id].descendants_ids))
        if descendant_changed:
            if node_id in self._changes.changed_descendants_map:
                self._changes.changed_descendants_map[node_id].update(descendant_ids)
            else:
                self._changes.changed_descendants_map[node_id] = descendant_ids
