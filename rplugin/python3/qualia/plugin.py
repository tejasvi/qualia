from time import time, sleep
from typing import Optional

from pynvim import plugin, Nvim, autocmd, command, attach, function

from qualia.config import _FZF_LINE_DELIMITER, NVIM_DEBUG_PIPE
from qualia.driver import PluginDriver
from qualia.models import NodeId
from qualia.services.search import matching_nodes_content, fzf_input_line
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import Database, exception_traceback, normalized_prefixes, save_root_view, logger, \
    get_node_content
from qualia.utils.plugin_utils import get_orphan_node_ids, PluginUtils
from qualia.utils.perf_utils import start_time


@plugin
class Qualia(PluginDriver):
    def __init__(self, nvim: Nvim, ide_debugging: bool = False):
        # First task on load
        try:
            bootstrap()
            super().__init__(nvim, ide_debugging)
        except Exception as e:
            logger.critical("Error during initialization" + exception_traceback(e))

    @autocmd("TextChanged,FocusGained,BufEnter,InsertLeave,BufLeave,BufFilePost,BufAdd,CursorHold", pattern='*.q.md',
             sync=True, allow_nested=False, eval=None)
    def trigger_sync(self, *_args) -> None:
        if self.ide_debugging or not self.should_continue(False):
            return
        try:
            self.main(None, None)
        except Exception as e:
            self.nvim.err_write(
                "\nSomething went wrong :(\n\n" + exception_traceback(e))

    @command("TriggerSync", sync=True)
    def trigger_sync_command(self) -> None:
        self.nvim.out_write("Begin\n")
        if self.should_continue(True):
            self.main(None, None)

    @command("NavigateNode", sync=True, nargs='?')
    def navigate_cur_node(self, args: list[NodeId] = None) -> None:
        node_id = (args and args[0]) or self.line_info(self.current_line_number()).node_id
        self.navigate_node(node_id, False)

    @command("HoistNode", sync=True)
    def hoist_node(self) -> None:
        line_num = self.current_line_number()
        view = self.line_node_view(line_num)
        with Database() as cursors:
            save_root_view(view, cursors.views)
        self.navigate_node(view.main_id, True)

    @command("ToggleParser", sync=True)
    def toggle_parser(self) -> None:
        self.enabled = not self.enabled
        if self.enabled:
            self.trigger_sync()

    @command("ToggleFold", sync=True, nargs='?')
    def toggle_fold(self, args: list[int] = None) -> None:
        cur_line_info = self.line_info(self.current_line_number())

        if cur_line_info is self.line_info(0):
            self.print_message("Can't toggle top level node")
        else:
            cur_context = cur_line_info.context
            cur_node_id = cur_line_info.node_id
            currently_expanded = cur_context[cur_node_id] is not None

            should_expand: Optional[bool] = bool(args[0]) if args else None
            if should_expand is None:
                should_expand = not currently_expanded
            if currently_expanded != should_expand:
                cur_context[cur_node_id] = (
                        cur_context[cur_node_id] or {}) if should_expand else None
                view = self.line_node_view(0)
                self.main(view, None)

    @command("FoldLevel", sync=True, nargs=1)
    def fold_level(self, args: list[str]) -> None:
        try:
            fold_level = int(args[0])
            assert fold_level > 0
        except (AssertionError, ValueError):
            self.print_message("Minimum fold level should be 1. Argument list provided: ", args)
        else:
            self.main(None, fold_level)

    @command("TransposeNode", sync=True, nargs='?')
    def transpose(self, args: list[str] = None) -> None:
        currently_transposed = self.buffer_transposed(self.nvim.current.buffer.name)
        node_id = self.line_info(self.current_line_number()).node_id
        try:
            replace_buffer = False if args and int(args[0]) else True
        except ValueError:
            self.print_message(
                "Optional argument to specify buffer replacement should be 1 or 0. Provided argument list: ", args)
        else:
            self.replace_with_file(self.node_id_to_filepath(node_id, not currently_transposed), replace_buffer)

    @command(PluginUtils.fzf_sink_command, nargs=1, sync=True)
    def fzf_sink(self, selections: list[str]):
        for i, selected in enumerate(selections):
            node_id = NodeId(selected[:selected.index(_FZF_LINE_DELIMITER)])
            node_filepath = self.node_id_to_filepath(node_id, False)
            if i == 0:
                self.replace_with_file(node_filepath, False)
            else:
                self.nvim.command(f"edit {node_filepath}")

    @command("SearchQualia", sync=True, nargs='*')
    def search_nodes(self, query_strings: list[str]) -> None:
        prefixes = normalized_prefixes(' '.join(query_strings))
        fzf_lines = matching_nodes_content(prefixes)
        self.fzf_run(fzf_lines, ' '.join(query_strings))

    @command("ListOrphans", sync=True)
    def list_orphans(self) -> None:
        with Database() as cursors:
            orphan_node_ids = get_orphan_node_ids(cursors)
            orphan_fzf_lines = [fzf_input_line(node_id, get_node_content(cursors, node_id)) for node_id in
                                orphan_node_ids]
        self.fzf_run(orphan_fzf_lines, '')

    @command("RemoveOrphans", sync=True, nargs='?')
    def remove_orphans(self, args: list[int] = None) -> None:
        skip_confirm = args and int(args[0])
        if skip_confirm or self.nvim.funcs.confirm("Remove orphans?", "&Be kind\n&Yes", 1) == 2:
            with Database() as cursors:
                for orphan_node_id in get_orphan_node_ids(cursors):
                    cursors.children.delete(orphan_node_id)

    @function("CurrentNodeId", sync=True)
    def current_node_id(self, line_num: list[int]) -> NodeId:
        return self.line_info(line_num[0] if line_num else self.current_line_number()).node_id

    @autocmd("VimEnter", pattern='*.q.md', sync=True)
    def log_startup_time(self, *_args) -> None:
        load_time = time() - start_time
        if True or load_time > 0.1:
            self.print_message(f"{round(load_time, 2)}s")


if __name__ == "__main__":
    nvim_debug = attach('socket', path=NVIM_DEBUG_PIPE)  # path=environ['NVIM_LISTEN_ADDRESS'])
    qualia_debug = Qualia(nvim_debug, True)
    sleep(1e5)
    while True:
        if qualia_debug.should_continue(False):
            qualia_debug.main(None, None)
        sleep(0.01)
