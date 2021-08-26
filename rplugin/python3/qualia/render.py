from __future__ import annotations

from sys import setrecursionlimit, getrecursionlimit
from typing import Optional

from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.config import DEBUG
from qualia.models import NodeId, View, NodeData, Tree, LastSeen, Cursors, LineInfo
from qualia.utils.common_utils import get_key_val, logger
from qualia.utils.render_utils import render_buffer, content_lines_to_buffer_lines


def render(root_view: View, buffer: Buffer, nvim: Nvim, cursors: Cursors, transposed: bool,
           fold_level: int) -> LastSeen:
    new_last_seen, new_content_lines = get_buffer_lines_from_view(root_view, cursors, transposed, fold_level)
    old_content_lines = render_buffer(buffer, new_content_lines, nvim)

    while True:
        try:
            if DEBUG:
                try:
                    new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.main_id,
                                                                         new_last_seen)
                    assert (not new_changes or (nvim.err_write(str(new_changes)) and False)), (
                        new_content_lines, new_changes)
                    re_last_seen, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, transposed,
                                                                                fold_level)
                    assert (new_content_lines == re_content_lines) or (
                            nvim.err_write('\n'.join(
                                new_content_lines + ['<TO>'] + re_content_lines)) and False), new_content_lines + [
                        '<TO>'] + re_content_lines
                except Exception as exp:
                    for _ in range(100):
                        new_root_view, new_changes = Process().process_lines(old_content_lines.copy(),
                                                                             root_view.main_id,
                                                                             new_last_seen)
                        new_root_view, new_changes = Process().process_lines(new_content_lines.copy(),
                                                                             root_view.main_id,
                                                                             new_last_seen)
                        re_last_seen, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, transposed,
                                                                                    fold_level)
                    raise exp

                # nvim.err_write(str((new_content_lines, old_content_lines)))
                logger.debug(new_content_lines, old_content_lines)
            break
        except RecursionError:
            if nvim.funcs.confirm("Too many nodes open. Expect slowdown on older machines. Continue?", "&Yes\n&Cancel",
                                  2) == 2:
                return LastSeen()
            setrecursionlimit(getrecursionlimit() * 2)
    return new_last_seen


def get_buffer_lines_from_view(buffer_view: View, cursors: Cursors, transposed: bool, fold_level: Optional[int]) -> \
        tuple[
            LastSeen, list[str]]:
    last_seen = LastSeen()
    buffer_lines: list[str] = []
    stack: list[tuple[NodeId, Tree, int, int, bool]] = [(buffer_view.main_id, {
        buffer_view.main_id: {} if buffer_view.sub_tree is None else buffer_view.sub_tree}, -1, 0, False)]
    while stack:
        cur_node_id, context, previous_indent_level, cur_nest_level, previously_ordered = stack.pop()

        content_lines = get_key_val(cur_node_id, cursors.content)
        if content_lines is None:
            continue

        children_ids: OrderedSet[NodeId] = OrderedSet(
            get_key_val(cur_node_id, cursors.parents if transposed else cursors.children) or [])

        if cur_node_id not in last_seen:
            last_seen[cur_node_id] = NodeData(content_lines, frozenset(children_ids))
        last_seen.line_info[len(buffer_lines)] = LineInfo(cur_node_id, context)

        buffer_children_context = context[cur_node_id]

        if fold_level is not None:
            if buffer_children_context is None:
                if cur_nest_level < fold_level:
                    buffer_children_context = {}
            else:
                if cur_nest_level == fold_level:
                    buffer_children_context = None

        expanded = not children_ids or buffer_children_context is not None

        ordered = expanded and ((len(children_ids) == 1) or previously_ordered) and previous_indent_level >= 0 and len(
            context) == 1
        current_indent_level = previous_indent_level if (ordered and previously_ordered) else previous_indent_level + 1

        if buffer_children_context is not None:
            context[cur_node_id] = children_context = {child_id: buffer_children_context.get(child_id, None)
                                                       for child_id in children_ids}
            for child_node_id in reversed(children_ids):  # sorted(children_ids, reverse=True):
                stack.append((child_node_id, children_context, current_indent_level, cur_nest_level + 1, ordered))

        buffer_lines += content_lines_to_buffer_lines(content_lines, cur_node_id, current_indent_level, expanded,
                                                      ordered)

    return last_seen, buffer_lines or ['']
