from difflib import SequenceMatcher
from typing import cast

from pynvim import Nvim
from pynvim.api import Buffer

from qualia.buffer import Process
from qualia.models import NodeId, View, NodeData
from qualia.states import ledger
from qualia.utils import get_key_val, Cursors, batch_undo, content_lines_to_buffer_lines


def render(root_view: View, buffer: Buffer, nvim: Nvim, cursors: Cursors) -> None:
    new_content_lines = get_buffer_lines_from_view(root_view, cursors)
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
            assert nvim.funcs.setline(min(old_i1, len(buffer)) + 1 + offset,
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

    new_root_view, new_changes = Process().process_lines(new_content_lines.copy(), root_view.root_id)
    assert not new_changes
    assert new_content_lines == get_buffer_lines_from_view(new_root_view, cursors)

    # nvim.err_write(str((new_content_lines, old_content_lines)))
    print(new_content_lines, old_content_lines)


def get_buffer_lines_from_view(view: View, cursors: Cursors) -> list[str]:
    ledger.clear()
    buffer_lines: list[str] = []
    stack: list[tuple[NodeId, dict, int]] = [(view.root_id, view.sub_tree, 0)]
    while stack:
        cur_node_id, sub_tree, level = stack.pop()

        content_lines = get_key_val(cur_node_id, cursors.content)
        if content_lines is None:
            continue

        children_id_list = get_key_val(cur_node_id, cursors.children)
        children_ids = frozenset() if children_id_list is None else frozenset(cast(list[NodeId], children_id_list))

        expanded = not children_ids or sub_tree
        if expanded:
            for child_node_id in sorted(children_ids, reverse=True):
                stack.append(
                    (child_node_id, sub_tree[child_node_id] if child_node_id in sub_tree else {}, level + 1))

        buffer_id, node_buffer_lines = content_lines_to_buffer_lines(content_lines, cur_node_id, level,
                                                                     expanded)
        buffer_lines += node_buffer_lines

        if cur_node_id not in ledger:
            ledger[cur_node_id] = NodeData(content_lines, children_ids, buffer_id)
            ledger.buffer_node_id_map[buffer_id] = cur_node_id

    return buffer_lines
