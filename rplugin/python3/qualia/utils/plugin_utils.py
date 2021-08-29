from collections import defaultdict
from os.path import basename
from pathlib import Path
from time import sleep
from typing import Optional, Any, cast

from lmdb import Cursor
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import NVIM_DEBUG_PIPE, _FZF_LINE_DELIMITER
from qualia.config import _FILE_FOLDER, _ROOT_ID_KEY
from qualia.models import NodeId, DuplicateNodeException, UncertainNodeChildrenException, View, BufferId, LastSeen, \
    Cursors, LineInfo
from qualia.utils.common_utils import get_key_val, file_name_to_node_id, node_id_to_hex, logger, get_node_descendants, \
    exception_traceback


class PluginUtils:

    def __init__(self, nvim: Nvim, debugging: bool):
        self.nvim = nvim
        self.ide_debugging = nvim.eval('v:servername') == NVIM_DEBUG_PIPE or debugging
        self.highlight_ns = nvim.funcs.nvim_create_namespace("qualia")

        self.changedtick: dict[BufferId, int] = defaultdict(lambda: -1)
        self.undo_seq: dict[BufferId, int] = {}
        self.buffer_last_seen: dict[BufferId, LastSeen] = defaultdict(LastSeen)
        self.last_git_sync = 0.
        self.enabled: bool = True

    def print_message(self, *args: Any):
        text = ' - '.join([str(text) for text in args])
        if self.ide_debugging:
            logger.debug(text)
        else:
            self.nvim.out_write(text + '\n')

    def replace_with_file(self, filepath: str, replace_buffer: bool) -> None:
        command = f"silent! edit! {filepath} | normal lh"  # wiggle closes FZF popup
        if replace_buffer:
            command = "bdelete | " + command
        self.nvim.command(command)

    def navigate_node(self, node_id: NodeId, replace_buffer: bool) -> None:
        transposed = self.buffer_transposed(self.nvim.current.buffer.name)
        filepath = self.node_id_to_filepath(node_id, transposed)
        if self.nvim.current.buffer.name != filepath:
            self.replace_with_file(filepath, replace_buffer)

    def process_filepath(self, buffer_name: str, cursors: Cursors, view) -> tuple[bool, bool, NodeId]:
        switched_buffer = False
        try:
            main_id, transposed = self.resolve_main_id(buffer_name, cursors.content)
        except ValueError:
            main_id, transposed = self.navigate_root_node(buffer_name, cursors.metadata)
            switched_buffer = True
        if view and main_id != view.main_id:
            self.navigate_node(view.main_id, True)
            switched_buffer = True
        return switched_buffer, transposed, main_id

    def navigate_root_node(self, buffer_name: str, metadata_cursor: Cursor) -> tuple[NodeId, bool]:
        transposed = self.buffer_transposed(basename(buffer_name))
        root_id = cast(NodeId, get_key_val(_ROOT_ID_KEY, metadata_cursor, True))
        self.replace_with_file(self.node_id_to_filepath(root_id, transposed), True)
        # self.print_message("Redirecting to root node")
        return root_id, transposed

    def current_buffer_id(self) -> Optional[BufferId]:
        buffer_number: int = self.nvim.current.buffer.number
        try:
            file_path = self.nvim.eval("resolve(expand('%:p'))")
        except OSError:
            return None
        else:
            return buffer_number, Path(file_path).as_posix()

    def current_line_number(self) -> int:
        return self.nvim.funcs.line('.') - 1

    def handle_duplicate_node(self, buffer: Buffer, exp: DuplicateNodeException):
        self.nvim.command("set nowrite")
        self.print_message(
            f"Duplicate siblings at lines {', '.join([str(first_line) for first_line, _ in exp.line_ranges])}")
        for node_locs in exp.line_ranges:
            for line_num in range(node_locs[0], node_locs[1]):
                self.highlight_line(buffer.number, line_num)

    def highlight_line(self, buffer_number: int, line_num: int) -> None:
        self.nvim.funcs.nvim_buf_add_highlight(buffer_number, self.highlight_ns, "ErrorMsg", line_num, 0, -1)

    def handle_uncertain_node_descendant(self, buffer: Buffer, exp: UncertainNodeChildrenException, last_seen: LastSeen):
        self.nvim.command("set nowrite")
        start_line_num, end_line_num = exp.line_range
        for line_num in range(start_line_num, min(end_line_num, start_line_num + 50)):
            self.highlight_line(buffer.number, line_num)
        choice = self.nvim.funcs.confirm("Uncertain state", "&Pause parsing\n&Continue", 1)
        if choice == 2:
            last_seen.pop_data(exp.node_id)
            return True
        else:
            self.enabled = False
            return False

    def delete_highlights(self, buffer_numer) -> None:
        self.nvim.funcs.nvim_buf_clear_namespace(buffer_numer, self.highlight_ns, 0, -1)

    def line_info(self, line_num) -> LineInfo:
        buffer_id = self.current_buffer_id()
        assert buffer_id is not None
        line_data = self.buffer_last_seen[buffer_id].line_info
        if line_data is not None:
            for line_number in range(line_num, -1, -1):
                if line_number in line_data:
                    return line_data[line_number]
        raise Exception("Current line info not found " + str(line_data))

    def line_node_view(self, line_num) -> View:
        line_info = self.line_info(line_num)
        node_id = line_info.node_id
        view = View(node_id, line_info.context[node_id])
        return view

    @staticmethod
    def buffer_transposed(buffer_name: str) -> bool:
        return basename(buffer_name)[0] == "~"

    @staticmethod
    def node_id_to_filepath(root_id: NodeId, transposed) -> str:
        file_name = node_id_to_hex(root_id) + ".q.md"
        if transposed:
            file_name = '~' + file_name
        return _FILE_FOLDER.joinpath(file_name).as_posix()

    @staticmethod
    def resolve_main_id(buffer_name: str, content_cursor: Cursor) -> tuple[NodeId, bool]:
        file_name = basename(buffer_name)
        transposed = PluginUtils.buffer_transposed(buffer_name)
        if transposed:
            file_name = file_name[1:]
        main_id = file_name_to_node_id(file_name, '.q.md')
        if get_key_val(main_id, content_cursor, False) is None:
            raise ValueError(buffer_name)
        return main_id, transposed

    def should_continue(self, force: bool) -> bool:
        if not force and self.ide_debugging:
            sleep(0.1)
            if self.nvim.funcs.mode().startswith("i"):
                return False

        if not self.enabled or (
                not force and self.nvim.funcs.mode().startswith("i")) or not self.nvim.current.buffer.name.endswith(
            ".q.md"):
            return False

        undotree = self.nvim.funcs.undotree()

        if (self.ide_debugging and undotree["synced"] == 0) or (undotree["seq_cur"] < undotree["seq_last"]):
            return False

        buffer_id = self.current_buffer_id()
        if buffer_id is None:
            return False
        if buffer_id in self.undo_seq and undotree["seq_cur"] - self.undo_seq[buffer_id] > 1:
            self.buffer_last_seen.pop(buffer_id)
        self.undo_seq[buffer_id] = undotree["seq_cur"]

        # Undo changes changedtick so check that before to pop last_seen
        try:
            changedtick = self.nvim.eval("b:changedtick")
        except OSError as e:
            self.print_message(exception_traceback(e))
        else:
            if not force and changedtick == self.changedtick[buffer_id]:
                return False
            else:
                self.changedtick[buffer_id] = changedtick

        return True

    fzf_sink_command = "NodeFzfSink"

    def fzf_run(self, fzf_lines: list[str], query: str) -> None:
        self.nvim.call("fzf#run",
                       {'source': fzf_lines, 'sink': self.fzf_sink_command, 'window': {'width': 0.95, 'height': 0.98},
                        'options': ['--delimiter', _FZF_LINE_DELIMITER, '--with-nth', '2..',
                                    '--query', query]})


def get_orphan_node_ids(cursors: Cursors) -> list[NodeId]:
    root_id = cast(NodeId, get_key_val(_ROOT_ID_KEY, cursors.metadata, True))
    visited_node_ids = {root_id}

    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()
        node_children_ids = get_node_descendants(cursors, node_id, False)
        if node_children_ids:
            node_stack.extend((child_id for child_id in node_children_ids if child_id not in visited_node_ids))
            visited_node_ids.update(node_children_ids)
    orphan_node_ids = [node_id_bytes.decode() for node_id_bytes in cursors.children.iternext(values=False) if
                       node_id_bytes.decode() not in visited_node_ids]
    return orphan_node_ids
