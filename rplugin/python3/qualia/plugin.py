from logging import getLogger
from time import sleep
from typing import Optional

from orderedset._orderedset import OrderedSet
from pynvim import plugin, Nvim, autocmd, command, attach, function

from qualia.config import _FZF_LINE_DELIMITER, NVIM_DEBUG_PIPE
from qualia.driver import PluginDriver
from qualia.models import NodeId
from qualia.services.search import matching_nodes_content, fzf_input_line
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import Database, exception_traceback, normalized_search_prefixes, save_root_view, \
    get_node_content_lines, delete_node, logger, get_node_descendants, set_node_descendants
from qualia.utils.plugin_utils import get_orphan_node_ids, PluginUtils


@plugin
class Qualia(PluginDriver):
    def __init__(self, nvim: Nvim, ide_debugging: bool = False):
        try:
            bootstrap()
            super().__init__(nvim, ide_debugging)
        except Exception as e:
            logger.critical("Error during initialization" + exception_traceback(e))
            raise e

    @autocmd("TextChanged,FocusGained,BufEnter,InsertLeave,BufLeave,BufFilePost,BufAdd,CursorHold", pattern='*.q.md',
             sync=True, allow_nested=False, eval=None)
    def trigger_sync(self, *args) -> None:
        logger.critical("Trigger")
        if self.ide_debugging or not self.should_continue(True if args and int(args[0]) else False):
            return
        try:
            self.main(None, None)
        except Exception as e:
            self.nvim.err_write(
                "\nSomething went wrong :(\n\n" + exception_traceback(e))

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

    @command("ToggleBufferSync", sync=True)
    def toggle_parser(self) -> None:
        self.enabled = not self.enabled
        if self.enabled:
            self.trigger_sync(True)
        self.print_message("Buffer sync paused" if self.enabled else "Buffer sync enabled")

    @command("ElevateNode", sync=True)  # TODO: Range
    def move_up(self) -> None:
        current_line_number = self.current_line_number()
        try:
            ancestory = self.node_ancestory_info(current_line_number, 2)
        except Exception as e:
            self.print_message("Can't move further up.")
            logger.debug(exception_traceback(e))
            return

        cur_line_info = ancestory[0]
        parent_line_info = ancestory[1]

        transposed = self.buffer_transposed(self.nvim.current.buffer.name)
        cur_node_id = cur_line_info.node_id
        parent_id = cur_line_info.parent_view.main_id
        grandparent_id = parent_line_info.parent_view.main_id

        with Database() as cursors:
            cur_node_siblings = get_node_descendants(cursors, parent_id, transposed, False)
            cur_node_siblings.remove(cur_node_id)
            set_node_descendants(parent_id, cur_node_siblings, cursors, transposed)

            parent_node_siblings_list = list(get_node_descendants(cursors, grandparent_id, transposed, False))
            parent_node_siblings_list.insert(parent_node_siblings_list.index(parent_id) + 1, cur_node_id)
            set_node_descendants(grandparent_id, OrderedSet(parent_node_siblings_list), cursors, transposed)

        self.trigger_sync(True)

    @command("ToggleFold", sync=True, nargs='?')
    def toggle_fold(self, args: list[int] = None) -> None:
        cur_line_info = self.line_info(self.current_line_number())

        if cur_line_info is self.line_info(0):
            self.print_message("Can't toggle top level node")
        else:
            cur_context = cur_line_info.parent_view.sub_tree
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
        prefixes = normalized_search_prefixes(' '.join(query_strings))
        fzf_lines = matching_nodes_content(prefixes)
        self.fzf_run(fzf_lines, ' '.join(query_strings))

    @command("ListOrphans", sync=True)
    def list_orphans(self) -> None:
        with Database() as cursors:
            orphan_fzf_lines = [fzf_input_line(node_id, get_node_content_lines(cursors, node_id)) for node_id in
                                get_orphan_node_ids(cursors)]
        self.fzf_run(orphan_fzf_lines, '')

    @command("RemoveOrphans", sync=True, nargs='?')
    def remove_orphans(self, args: list[int] = None) -> None:
        skip_confirm = args and int(args[0])
        if not (skip_confirm or self.nvim.funcs.confirm("Remove orphans?", "&Be kind\n&Yes", 1) == 2):
            return
        with Database() as cursors:
            for orphan_node_id in get_orphan_node_ids(cursors):
                delete_node(cursors, orphan_node_id)

    @function("CurrentNodeId", sync=True)
    def current_node_id(self, line_num: list[int]) -> NodeId:
        return self.line_info(line_num[0] if line_num else self.current_line_number()).node_id


if __name__ == "__main__":
    from qualia.config import _LOGGER_NAME
    from qualia.utils.init_utils import setup_logger

    _logger = getLogger(_LOGGER_NAME)
    setup_logger(_logger)

    nvim_debug = attach('socket', path=NVIM_DEBUG_PIPE)  # path=environ['NVIM_LISTEN_ADDRESS'])
    qualia_debug = Qualia(nvim_debug, True)
    while True:
        try:
            if qualia_debug.should_continue(False):
                qualia_debug.main(None, None)
        except OSError as exp:
            raise exp

        sleep(0.01)
