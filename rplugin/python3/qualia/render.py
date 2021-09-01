from __future__ import annotations

from typing import Optional

from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.config import DEBUG, _SORT_SIBLINGS
from qualia.models import NodeId, View, NodeData, Tree, LastSync, Cursors, LineInfo
from qualia.utils.common_utils import logger, get_node_descendants, get_node_content
from qualia.utils.render_utils import render_buffer, content_lines_to_buffer_lines


def render(root_view: View, buffer: Buffer, nvim: Nvim, cursors: Cursors, transposed: bool,
           fold_level: Optional[int]) -> LastSync:
    new_last_sync, new_content_lines = get_buffer_lines_from_view(root_view, cursors, transposed, fold_level)
    old_content_lines = render_buffer(buffer, new_content_lines, nvim)

    if DEBUG:
        try:
            new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.main_id,
                                                                 new_last_sync, cursors)
            assert not new_changes, '\n'.join(
                map(str, (old_content_lines, new_content_lines, new_changes, new_last_sync)))
            re_last_sync, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, transposed, fold_level)
            assert new_content_lines == re_content_lines, '\n'.join(new_content_lines + ['<TO>'] + re_content_lines)
        except Exception as exp:
            for _ in range(100):
                new_root_view, new_changes = Process().process_lines(old_content_lines.copy(), root_view.main_id,
                                                                     new_last_sync, cursors)
                new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.main_id,
                                                                     new_last_sync, cursors)
                re_last_sync, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, transposed,
                                                                            fold_level)
            raise exp
        logger.debug(f"{(new_content_lines, old_content_lines)}")
    return new_last_sync


def get_buffer_lines_from_view(buffer_view: View, cursors: Cursors, transposed: bool, fold_level: Optional[int]) -> \
        tuple[
            LastSync, list[str]]:
    last_sync = LastSync()
    buffer_lines: list[str] = []
    stack: list[tuple[NodeId, Tree, int, int, bool]] = [(buffer_view.main_id, {
        buffer_view.main_id: {} if buffer_view.sub_tree is None else buffer_view.sub_tree}, -1, 0, False)]
    while stack:
        cur_node_id, context, previous_indent_level, cur_nest_level, previously_ordered = stack.pop()

        content_lines = get_node_content(cursors, cur_node_id)
        descendant_ids = get_node_descendants(cursors, cur_node_id, transposed, True)

        if cur_node_id not in last_sync:
            last_sync[cur_node_id] = NodeData(content_lines, OrderedSet(descendant_ids))
        last_sync.line_info[len(buffer_lines)] = LineInfo(cur_node_id, context)

        buffer_descendant_context = context[cur_node_id]

        if fold_level is not None:
            if buffer_descendant_context is None:
                if cur_nest_level < fold_level:
                    buffer_descendant_context = {}
            else:
                if cur_nest_level == fold_level:
                    buffer_descendant_context = None

        expanded = not descendant_ids or buffer_descendant_context is not None

        ordered = expanded and (
                (len(descendant_ids) == 1) or previously_ordered) and previous_indent_level >= 0 and len(
            context) == 1
        current_indent_level = previous_indent_level if (ordered and previously_ordered) else previous_indent_level + 1

        if buffer_descendant_context is not None:
            context[cur_node_id] = descendant_context = {
                descendant_id: buffer_descendant_context.get(descendant_id)
                for descendant_id in descendant_ids}
            for descendant_node_id in sorted(descendant_ids, reverse=True) if _SORT_SIBLINGS else reversed(descendant_ids):
                stack.append(
                    (descendant_node_id, descendant_context, current_indent_level, cur_nest_level + 1, ordered,))

        buffer_lines += content_lines_to_buffer_lines(content_lines, cur_node_id, current_indent_level, expanded,
                                                      ordered, cursors)

    return last_sync, buffer_lines or ['']
