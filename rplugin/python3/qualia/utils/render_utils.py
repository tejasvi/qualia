from base64 import b64encode
from difflib import SequenceMatcher
from typing import Iterator, Callable, Union, cast, Optional, TYPE_CHECKING

from qualia.config import DEBUG, _EXPANDED_BULLET, _COLLAPSED_BULLET, NEST_LEVEL_SPACES, _SHORT_BUFFER_ID, \
    _BUFFER_ID_STORE_BYTES
from qualia.models import NodeId, BufferNodeId, Cursors, BufferContentSetter, Li
from qualia.utils.common_utils import logger, get_key_val, set_key_val, get_node_descendants

if TYPE_CHECKING:
    from pynvim.api import Buffer
    from pynvim import Nvim


def batch_undo(nvim):
    # type:(Nvim) -> Iterator[None]
    first_change = True
    while True:
        if first_change:
            first_change = False
        else:
            nvim.command("undojoin")
        yield


def get_replace_buffer_line(nvim):
    # type:(Nvim) -> BufferContentSetter
    setline = nvim.funcs.setline

    def replace_buffer_line(zero_idx_line_num: int, content: Union[str, Li]) -> None:
        assert setline(zero_idx_line_num + 1, content) == 0

    return replace_buffer_line


def render_buffer(buffer, new_content_lines, nvim):
    # type:(Buffer, Li, Nvim) -> Li
    old_content_lines = cast(Li, list(buffer))
    # Pre-Check common state with == (100x faster than loop)
    if old_content_lines != new_content_lines:
        line_conflict = True
        first_diff_line_num = -1
        for first_diff_line_num, (old_line, new_line) in enumerate(zip(old_content_lines, new_content_lines)):
            if old_line != new_line:
                break
        else:
            first_diff_line_num += 1
            line_conflict = False

        undojoin = batch_undo(nvim)
        next(undojoin)

        set_buffer_line = get_replace_buffer_line(nvim)

        line_new_end: Optional[int]
        line_old_end: Optional[int]

        if line_conflict:
            line_new_end, line_old_end = different_item_idxs_from_end(new_content_lines, old_content_lines,
                                                                      first_diff_line_num)
        else:
            line_new_end, line_old_end = None, None

        if line_conflict and first_diff_line_num in (line_old_end, line_new_end):
            if first_diff_line_num == line_old_end:
                assert line_new_end is not None
                set_buffer_line(first_diff_line_num, new_content_lines[first_diff_line_num])
                buffer[first_diff_line_num + 1:first_diff_line_num + 1] = new_content_lines[
                                                                          first_diff_line_num + 1:line_new_end + 1]
            else:
                assert line_old_end is not None
                set_buffer_line(first_diff_line_num, new_content_lines[first_diff_line_num])
                del buffer[first_diff_line_num + 1:line_old_end + 1]
        else:
            if (len(old_content_lines) - first_diff_line_num) * (len(new_content_lines) - first_diff_line_num) > 1e5:
                buffer[first_diff_line_num:] = new_content_lines[first_diff_line_num:]
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


def surgical_render(buffer, new_content_lines, replace_buffer_line, old_content_lines, undojoin):
    # type:(Buffer, Li, BufferContentSetter, Li, Iterator) -> None
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
            replace_buffer_line(min(old_i1 + offset, len(buffer)),
                                cast(Li, new_content_lines[new_i1:new_i1 + min_lines]))
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


def different_item_idxs_from_end(list1: list, list2: list, minimum_idx: int) -> tuple[int, int]:
    len1 = len(list1)
    len2 = len(list2)
    maximum_idx_rev = min(len1 - minimum_idx, len2 - minimum_idx) - 1
    assert maximum_idx_rev >= 0

    for i_rev, (item1, item2) in enumerate(zip(reversed(list1), reversed(list2))):
        if item1 != item2 or i_rev == maximum_idx_rev:
            break
    else:
        raise ValueError("Lists are same")

    idx1 = len(list1) - i_rev - 1
    idx2 = len(list2) - i_rev - 1

    return idx1, idx2


def content_lines_to_buffer_lines(content_lines: Li, node_id: NodeId, level: int, expanded: bool, ordered: bool,
                                  cursors: Cursors, transposed: bool) -> Li:
    if level == 0:
        buffer_lines = content_lines
    else:
        offset = 3 if ordered else 2
        space_count = NEST_LEVEL_SPACES * (level - 1) + offset
        space_prefix = ' ' * space_count
        buffer_lines = cast(Li, [space_prefix[:-offset]
                                 + ('1.' if ordered else (_EXPANDED_BULLET if expanded else _COLLAPSED_BULLET)) + ' '
                                 + buffer_node_tracker(node_id, transposed, cursors)
                                 + content_lines[0]])
        for idx, line in enumerate(content_lines[1:]):
            buffer_lines.append(space_prefix + line)
    return buffer_lines


def buffer_node_tracker(node_id: NodeId, transposed: bool, cursors: Cursors) -> str:
    has_other_ancestors = len(get_node_descendants(cursors, node_id, not transposed, True)) > 1
    return "[](" + (('T' if has_other_ancestors else 't') if transposed else ('N' if has_other_ancestors else 'n')
                    ) + node_to_buffer_id(node_id, cursors) + ")  "


# misplaced cursor when concealing wide characters (from base65536)
# Base64 stores 6 bits per letter. 000000 is represented as 'A'
_buffer_id_encoder: Callable[[bytes], BufferNodeId] = lambda a: cast(BufferNodeId, b64encode(a).decode().rstrip(
    "=").lstrip('A') or 'A')  # base65536.encode


def node_to_buffer_id(node_id: NodeId, cursors: Cursors) -> BufferNodeId:
    if not _SHORT_BUFFER_ID:
        return cast(BufferNodeId, node_id)
    buffer_node_id = cast(Optional[BufferNodeId], get_key_val(node_id, cursors.node_id_buffer_id, False))
    if buffer_node_id is None:
        if cursors.buffer_id_bytes_node_id.last():
            last_buffer_id_bytes = cursors.buffer_id_bytes_node_id.key()
            new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
            buffer_id_bytes = new_counter.to_bytes(_BUFFER_ID_STORE_BYTES, 'big')
        else:
            buffer_id_bytes = (0).to_bytes(_BUFFER_ID_STORE_BYTES, 'big')
        # base65536 doesn't output brackets https://qntm.org/safe
        # base65536 gives single character for 16bits == 2bytes
        buffer_node_id = _buffer_id_encoder(buffer_id_bytes)
        logger.debug(f"{node_id} {buffer_node_id}")
        set_key_val(node_id, buffer_node_id, cursors.node_id_buffer_id, True)
        set_key_val(buffer_id_bytes, node_id, cursors.buffer_id_bytes_node_id, True)
    return cast(BufferNodeId, buffer_node_id)
