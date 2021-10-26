from __future__ import annotations

from typing import Optional, cast, TYPE_CHECKING

from orderedset import OrderedSet

from qualia.config import DEBUG, _SORT_SIBLINGS
from qualia.models import NodeId, View, NodeData, LastSync, LineInfo, Li, InvalidNodeId, MinimalDb
from qualia.sync import ParseProcess
from qualia.utils.render_utils import render_buffer, content_lines_to_buffer_lines

if TYPE_CHECKING:
    from pynvim import Nvim
    from pynvim.api import Buffer


def render(root_view, buffer, nvim, db, transposed, fold_level):
    # type:(View, Buffer, Nvim, MinimalDb, bool, Optional[int]) -> LastSync
    new_last_sync, new_content_lines = get_buffer_lines_from_view(root_view, db, transposed, fold_level)
    old_content_lines = render_buffer(buffer, new_content_lines, nvim)

    if DEBUG:
        try:
            new_root_view, new_changes = ParseProcess().process_lines(cast(Li, new_content_lines.copy()),
                                                                      root_view.main_id,
                                                                      new_last_sync, db, transposed)
            assert not new_changes, '\n'.join(
                map(str, (old_content_lines, new_content_lines, new_changes, new_last_sync)))
            re_last_sync, re_content_lines = get_buffer_lines_from_view(new_root_view, db, transposed, fold_level)
            assert new_content_lines == re_content_lines, '\n'.join(new_content_lines + ['<TO>'] + re_content_lines)
        except Exception as exp:
            for _ in range(100):
                new_root_view, new_changes = ParseProcess().process_lines(cast(Li, new_content_lines.copy()),
                                                                          root_view.main_id,
                                                                          new_last_sync, db, transposed)
                new_root_view, new_changes = ParseProcess().process_lines(cast(Li, new_content_lines.copy()),
                                                                          root_view.main_id,
                                                                          new_last_sync, db, transposed)
                re_last_sync, re_content_lines = get_buffer_lines_from_view(new_root_view, db, transposed,
                                                                            fold_level)
            raise exp
    return new_last_sync


def get_buffer_lines_from_view(buffer_view: View, db: MinimalDb, transposed: bool,
                               fold_level: Optional[int]) -> tuple[LastSync, Li]:
    last_sync = LastSync(None)
    buffer_lines = cast(Li, [])
    stack: list[tuple[NodeId, View, int, int, bool, int]] = [(buffer_view.main_id, View(cast(NodeId, InvalidNodeId()), {
        buffer_view.main_id: {} if buffer_view.sub_tree is None else buffer_view.sub_tree}, transposed), -1, 0, False,
                                                              1)]
    while stack:
        cur_node_id, parent_view, previous_indent_level, cur_nest_level, previously_ordered, parent_sibling_count = stack.pop()

        content_lines = db.get_node_content_lines(cur_node_id)
        descendant_ids = db.get_node_descendants(cur_node_id, transposed, True)

        cur_context = parent_view.sub_tree
        assert cur_context is not None

        if cur_node_id not in last_sync:
            last_sync[cur_node_id] = NodeData(content_lines, OrderedSet(descendant_ids))
        last_sync.line_info[len(buffer_lines)] = LineInfo(cur_node_id, parent_view, cur_nest_level)

        buffer_descendant_context = cur_context[cur_node_id]

        if fold_level is not None:
            if buffer_descendant_context is None:
                if cur_nest_level < fold_level:
                    buffer_descendant_context = {}
            else:
                if cur_nest_level == fold_level:
                    buffer_descendant_context = None

        expanded = not descendant_ids or buffer_descendant_context is not None

        ordered = expanded and (
                (len(descendant_ids) == 1) or previously_ordered) and previous_indent_level > 0 and len(
            cur_context) == 1 and parent_sibling_count == 1
        current_indent_level = previous_indent_level if (ordered and previously_ordered) else previous_indent_level + 1

        if buffer_descendant_context is not None:
            cur_context[cur_node_id] = descendant_context = {
                descendant_id: buffer_descendant_context.get(descendant_id)
                for descendant_id in descendant_ids}
            for descendant_node_id in sorted(descendant_ids, reverse=True) if _SORT_SIBLINGS else reversed(
                    descendant_ids):
                stack.append(
                    (
                        descendant_node_id, View(cur_node_id, descendant_context, transposed), current_indent_level,
                        cur_nest_level + 1,
                        ordered, len(cur_context)))

        buffer_lines += content_lines_to_buffer_lines(content_lines, cur_node_id, current_indent_level, expanded,
                                                      ordered, db, transposed)

    return last_sync, buffer_lines or cast(Li, [''])
