from difflib import SequenceMatcher
from typing import Iterator, Callable, Union

from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import DEBUG, _EXPANDED_BULLET, _COLLAPSED_BULLET, NEST_LEVEL_SPACES
from qualia.models import NodeId, BufferNodeId
from qualia.utils.common_utils import logger


def batch_undo(nvim: Nvim) -> Iterator[None]:
    first_change = True
    while True:
        if first_change:
            first_change = False
        else:
            nvim.command("undojoin")
        yield


def get_replace_buffer_line(nvim: Nvim) -> Callable[[int, Union[str, list[str]]], None]:
    setline = nvim.funcs.setline

    def replace_buffer_line(zero_indexed: int, content: Union[str, list[str]]) -> None:
        assert setline(zero_indexed + 1, content) == 0

    return replace_buffer_line


def render_buffer(buffer: Buffer, new_content_lines: list[str], nvim: Nvim) -> list[str]:
    old_content_lines = list(buffer)
    # Pre-Check common state with == (100x faster than loop)
    if old_content_lines != new_content_lines:
        line_num = 0
        for line_num, (old_line, new_line) in enumerate(zip(old_content_lines, new_content_lines)):
            if old_line != new_line:
                break

        undojoin = batch_undo(nvim)
        next(undojoin)

        set_buffer_line = get_replace_buffer_line(nvim)

        line_new_end, line_old_end = different_item_from_end(new_content_lines, old_content_lines, line_num)

        if line_num in (line_old_end, line_new_end):
            if line_num == line_old_end:
                set_buffer_line(line_num, new_content_lines[line_num])
                buffer[line_num + 1:line_num + 1] = new_content_lines[line_num + 1:line_new_end + 1]
            else:
                set_buffer_line(line_num, new_content_lines[line_num])
                del buffer[line_num + 1:line_old_end + 1]
        else:
            if (len(old_content_lines) - line_num) * (len(new_content_lines) - line_num) > 1e5:
                buffer[line_num:] = new_content_lines[line_num:]
            else:
                logger.debug("Surgical")
                surgical_render(buffer, new_content_lines, set_buffer_line, old_content_lines, undojoin)
    if DEBUG:
        try:
            assert new_content_lines == list(buffer)
        except AssertionError:
            buffer[:] = old_content_lines
            render_buffer(buffer, new_content_lines, nvim)
    return old_content_lines


def surgical_render(buffer: Buffer, new_content_lines: list[str],
                    replace_buffer_line: Callable[[int, Union[str, list[str]]], None],
                    old_content_lines: list[str], undojoin: Iterator) -> None:
    offset = 0
    for opcode, old_i1, old_i2, new_i1, new_i2 in SequenceMatcher(a=old_content_lines, b=new_content_lines,
                                                                  autojunk=False).get_opcodes():
        if opcode == "equal":
            continue
        next(undojoin)
        if opcode == "replace":
            num_old_lines = old_i2 - old_i1
            num_new_lines = new_i2 - new_i1
            min_lines = min(num_old_lines, num_new_lines)
            # setline preserves the marks unlike buffer[lnum] = "content"
            replace_buffer_line(min(old_i1 + offset, len(buffer)), new_content_lines[new_i1:new_i1 + min_lines])
            if num_new_lines != num_old_lines:
                next(undojoin)
                if num_new_lines > num_old_lines:
                    idx = old_i1 + min_lines + offset
                    buffer[idx:idx] = new_content_lines[new_i1 + min_lines: new_i2]
                elif num_new_lines < num_old_lines:
                    del buffer[old_i1 + min_lines + offset:old_i2 + offset]
            offset += num_new_lines - num_old_lines
        elif opcode == "insert":
            buffer[old_i1 + offset:old_i1 + offset] = new_content_lines[new_i1:new_i2]
            offset += new_i2 - new_i1
        elif opcode == "delete":
            del buffer[old_i1 + offset:old_i2 + offset]
            offset -= old_i2 - old_i1


def different_item_from_end(list1: list, list2: list, minimum_idx: int) -> tuple[int, int]:
    len1 = len(list1)
    len2 = len(list2)
    maximum_idx_rev = min(len1 - minimum_idx, len2 - minimum_idx) - 1

    for i_rev, (item1, item2) in enumerate(zip(reversed(list1), reversed(list2))):
        if item1 != item2 or i_rev == maximum_idx_rev:
            break
    else:
        raise ValueError("Lists are same")

    idx1 = len(list1) - i_rev - 1
    idx2 = len(list2) - i_rev - 1

    return idx1, idx2


def content_lines_to_buffer_lines(content_lines: list[str], node_id: NodeId, level: int, expanded: bool,
                                  ordered: bool) -> list[str]:
    buffer_id = node_to_buffer_id(node_id)
    if level == 0:
        buffer_lines = content_lines
    else:
        offset = 3 if ordered else 2
        space_count = NEST_LEVEL_SPACES * (level - 1) + offset
        space_prefix = ' ' * space_count
        buffer_lines = [space_prefix[:-offset]
                        + ('1.' if ordered else (_EXPANDED_BULLET if expanded else _COLLAPSED_BULLET))
                        + f" [](q://{buffer_id})  "
                        + content_lines[0]]
        for idx, line in enumerate(content_lines[1:]):
            buffer_lines.append(space_prefix + line)
    return buffer_lines


def node_to_buffer_id(node_id: NodeId) -> BufferNodeId:
    return BufferNodeId(node_id)
    # buffer_node_id = get_key_val(node_id, cursors.buffer_to_node_id)
    # if buffer_node_id is None:
    #     if cursors.buffer_to_node_id.last():
    #         last_buffer_id_bytes = cursors.buffer_to_node_id.key()
    #         new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
    #         buffer_id_bytes = new_counter.to_bytes(32, 'big').decode()
    #     else:
    #         buffer_id_bytes = (0).to_bytes(32, 'big')
    #     buffer_node_id = base65536.encode(buffer_id_bytes)
    #     # base65536 doesn't output brackets https://qntm.org/safe
    #     put_key_val(node_id, buffer_node_id, cursors.node_to_buffer_id)
    # return buffer_node_id