from __future__ import annotations
from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.config import DEBUG
from qualia.models import NodeId, View, NodeData, Tree, LastSeen, Cursors, LineInfo
from qualia.utils import get_key_val, content_lines_to_buffer_lines, render_buffer


def render(root_view: View, buffer: Buffer, nvim: Nvim, cursors: Cursors, inverted: bool) -> LastSeen:
    new_last_seen, new_content_lines = get_buffer_lines_from_view(root_view, cursors, inverted)
    old_content_lines = render_buffer(buffer, new_content_lines, nvim)

    if DEBUG:
        try:
            new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.main_id,
                                                                 new_last_seen)
            assert (not new_changes or (nvim.err_write(str(new_changes)) and False))
            re_last_seen, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, inverted)
            assert (new_content_lines == re_content_lines) or (
                    nvim.err_write('\n'.join(new_content_lines + ['<TO>'] + re_content_lines)) and False)
        except Exception as exp:
            for _ in range(100):
                new_root_view, new_changes = Process().process_lines(old_content_lines.copy(), root_view.main_id,
                                                                     new_last_seen)
                new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.main_id,
                                                                     new_last_seen)
                re_last_seen, re_content_lines = get_buffer_lines_from_view(new_root_view, cursors, inverted)
            raise exp

        # nvim.err_write(str((new_content_lines, old_content_lines)))
        print(new_content_lines, old_content_lines)
    return new_last_seen


def get_buffer_lines_from_view(view: View, cursors: Cursors, inverted: bool) -> tuple[LastSeen, list[str]]:
    last_seen = LastSeen()
    buffer_lines: list[str] = []
    stack: list[tuple[NodeId, Tree, int, bool]] = [(view.main_id, {view.main_id: view.sub_tree}, -1, False)]
    while stack:
        cur_node_id, context, previous_level, previously_ordered = stack.pop()

        content_lines = get_key_val(cur_node_id, cursors.content)
        if content_lines is None:
            continue

        children_ids: OrderedSet[NodeId] = OrderedSet(
            get_key_val(cur_node_id, cursors.parents if inverted else cursors.children) or [])

        if cur_node_id not in last_seen:
            last_seen[cur_node_id] = NodeData(content_lines, frozenset(children_ids))
        last_seen.line_info[len(buffer_lines)] = LineInfo(cur_node_id, context)

        buffer_children_context = context[cur_node_id]

        expanded = not children_ids or buffer_children_context is not None
        ordered = expanded and ((len(children_ids) == 1) or previously_ordered) and previous_level >= 0 and len(
            context) == 1
        current_level = previous_level if (ordered and previously_ordered) else previous_level + 1

        if buffer_children_context is not None:
            # if children_context is None:
            #     children_context = {}
            # else:
            #     for child_id in children_context.keys() - children_ids:
            #         children_context.pop(child_id)
            context[cur_node_id] = children_context = {child_id: buffer_children_context.get(child_id, None)
                                                       for child_id in children_ids}

            # if not children_context:
            #     children_context = {child_id: None for child_id in children_ids}
            for child_node_id in reversed(children_ids):  # sorted(children_ids, reverse=True):
                stack.append((child_node_id, children_context, current_level, ordered))

        buffer_lines += content_lines_to_buffer_lines(content_lines, cur_node_id, current_level, expanded, ordered)

    return last_seen, buffer_lines or ['']
