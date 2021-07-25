from difflib import SequenceMatcher
from typing import cast

from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.models import NodeId, View, NodeData, Tree, Ledger, Cursors
from qualia.utils import get_key_val, batch_undo, content_lines_to_buffer_lines


def render(root_view: View, buffer: Buffer, nvim: Nvim, ledger: Ledger, cursors: Cursors) -> None:
    new_content_lines = get_buffer_lines_from_view(root_view, ledger, cursors)
    old_content_lines = list(buffer)
    undojoin = batch_undo(nvim)

    offset = 0
    for opcode, old_i1, old_i2, new_i1, new_i2 in SequenceMatcher(a=old_content_lines, b=new_content_lines,
                                                                  autojunk=False).get_opcodes():
        if opcode == "replace":
            num_old_lines = old_i2 - old_i1
            num_new_lines = new_i2 - new_i1
            min_lines = min(num_old_lines, num_new_lines)
            # setline preserves the marks unlike buffer[lnum] = "content"
            next(undojoin)
            assert nvim.funcs.setline(min(old_i1 + offset, len(buffer)) + 1,
                                      new_content_lines[new_i1:new_i1 + min_lines]) == 0
            if num_new_lines > num_old_lines:
                next(undojoin)
                idx = old_i1 + min_lines + offset
                buffer[idx:idx] = new_content_lines[new_i1 + min_lines: new_i2]
            elif num_new_lines < num_old_lines:
                next(undojoin)
                del buffer[old_i1 + min_lines + offset:old_i2 + offset]
            offset += num_new_lines - num_old_lines
        elif opcode == "insert":
            next(undojoin)
            buffer[old_i1 + offset:old_i1 + offset] = new_content_lines[new_i1:new_i2]
            offset += new_i2 - new_i1
        elif opcode == "delete":
            next(undojoin)
            del buffer[old_i1 + offset:old_i2 + offset]
            offset -= old_i2 - old_i1

    assert new_content_lines == list(buffer)

    try:
        new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.root_id, ledger)
        assert (not new_changes or (nvim.err_write(str(new_changes)) and False))
        re_content_lines = get_buffer_lines_from_view(new_root_view, ledger, cursors)
        assert (new_content_lines == re_content_lines) or (
                nvim.err_write(str((new_content_lines, re_content_lines))) and False)
    except Exception as exp:
        for _ in range(100):
            new_root_view, new_changes = Process().process_lines(old_content_lines.copy(), root_view.root_id, ledger)
            new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.root_id, ledger)
            re_content_lines = get_buffer_lines_from_view(new_root_view, ledger, cursors)
        raise exp

    # nvim.err_write(str((new_content_lines, old_content_lines)))
    print(new_content_lines, old_content_lines)


def get_buffer_lines_from_view(view: View, ledger: Ledger, cursors: Cursors) -> list[str]:
    ledger.clear()
    buffer_lines: list[str] = []
    stack: list[tuple[NodeId, Tree, int, bool]] = [(view.root_id, {view.root_id: view.sub_tree}, -1, False)]
    while stack:
        cur_node_id, context, previous_level, previously_ordered = stack.pop()
        children_context = context[cur_node_id] if context and cur_node_id in context else None

        content_lines = get_key_val(cur_node_id, cursors.content)
        if content_lines is None:
            continue

        children_id_list = get_key_val(cur_node_id, cursors.children) or []
        children_ids = OrderedSet(cast(list[NodeId], children_id_list))

        expanded = not children_ids or (children_context is not None)

        ordered = expanded and ((len(children_ids) == 1) or previously_ordered) and previous_level >= 0 and len(
            context) == 1

        current_level = previous_level if (ordered and previously_ordered) else previous_level + 1

        if expanded:
            for child_node_id in reversed(children_ids):  # sorted(children_ids, reverse=True):
                stack.append((child_node_id, children_context, current_level, ordered))

        buffer_id, node_buffer_lines = content_lines_to_buffer_lines(content_lines, cur_node_id, current_level,
                                                                     expanded, ordered)
        buffer_lines += node_buffer_lines

        if cur_node_id not in ledger:
            ledger[cur_node_id] = NodeData(content_lines, frozenset(children_ids))

    return buffer_lines or ['']
