from collections import defaultdict
from os.path import basename
from pathlib import Path
from time import sleep
from typing import Optional, Any, cast

from lmdb import Cursor
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import NVIM_DEBUG_PIPE
from qualia.config import _FILE_FOLDER, _ROOT_ID_KEY
from qualia.models import NodeId, DuplicateNodeException, UncertainNodeChildrenException, View, BufferId, LastSeen, \
    Cursors, LineInfo
from qualia.utils.common_utils import get_key_val, file_name_to_node_id, node_id_to_hex, logger


class NvimUtils:
    def __init__(self, nvim: Nvim, debugging: bool):
        self.nvim = nvim
        self.debugging = nvim.eval('v:servername') == NVIM_DEBUG_PIPE or debugging
        self.children_ns = nvim.funcs.nvim_create_namespace("children")
        self.duplicate_ns = nvim.funcs.nvim_create_namespace("duplicates")

        self.changedtick: dict[BufferId, int] = defaultdict(lambda: -1)
        self.undo_seq: dict[BufferId, int] = {}
        self.buffer_last_seen: dict[BufferId, LastSeen] = defaultdict(LastSeen)
        self.last_git_sync = 0
        self.enabled: bool = True

    def print_message(self, *args: Any):
        text = ' - '.join([str(text) for text in args])
        if self.debugging:
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

    def navigate_filepath(self, buffer_name: str, cursors: Cursors, view) -> tuple[bool, bool, NodeId]:
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
        root_id = cast(NodeId, get_key_val(_ROOT_ID_KEY, metadata_cursor))
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
                self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.children_ns, "ErrorMsg", line_num, 0, -1)

    def handle_uncertain_node_children(self, buffer: Buffer, exp: UncertainNodeChildrenException, last_seen: LastSeen):
        self.nvim.command("set nowrite")
        start_line_num, end_line_num = exp.line_range
        for line_num in range(start_line_num, min(end_line_num, start_line_num + 50)):
            self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.children_ns, "ErrorMsg", line_num, 0, -1)
        choice = self.nvim.funcs.confirm("Uncertain state", "&Pause parsing\n&Continue", 1)
        if choice == 2:
            last_seen.pop_data(exp.node_id)
            return True
        else:
            self.enabled = False
            return False

    def delete_highlights(self, buffer_numer) -> None:
        for ns_id in (self.children_ns, self.duplicate_ns):
            self.nvim.funcs.nvim_buf_clear_namespace(buffer_numer, ns_id, 0, -1)

    def line_info(self, line_num) -> LineInfo:
        line_data = self.buffer_last_seen[self.current_buffer_id()].line_info
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
        transposed = NvimUtils.buffer_transposed(buffer_name)
        if transposed:
            file_name = file_name[1:]
        main_id = file_name_to_node_id(file_name, '.q.md')
        if get_key_val(main_id, content_cursor) is None:
            raise ValueError(buffer_name)
        return main_id, transposed

    def should_continue(self) -> bool:
        if self.debugging:
            sleep(0.1)
            if self.nvim.funcs.mode().startswith("i"):
                return False

        if not self.enabled or self.nvim.funcs.mode().startswith("i") or not self.nvim.current.buffer.name.endswith(
                ".q.md"):
            return False

        undotree = self.nvim.funcs.undotree()

        if (self.debugging and undotree["synced"] == 0) or (undotree["seq_cur"] < undotree["seq_last"]):
            return False

        buffer_id = self.current_buffer_id()
        if buffer_id is None:
            return False
        if buffer_id in self.undo_seq and undotree["seq_cur"] - self.undo_seq[buffer_id] > 1:
            self.buffer_last_seen.pop(buffer_id)
        self.undo_seq[buffer_id] = undotree["seq_cur"]

        # Undo changes changedtick so check that before to pop last_seen
        changedtick = self.nvim.eval("b:changedtick")
        if changedtick == self.changedtick[buffer_id]:
            return False
        else:
            self.changedtick[buffer_id] = changedtick

        return True
