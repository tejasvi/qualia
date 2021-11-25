from time import sleep
from typing import Optional

from orderedset import OrderedSet
from pynvim import plugin, Nvim, command, attach, function

from qualia.config import _FZF_LINE_DELIMITER, NVIM_DEBUG_PIPE
from qualia.database import Database
from qualia.driver import PluginDriver
from qualia.models import NodeId
from qualia.services.search import matching_nodes_content, fzf_input_line
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import exception_traceback, normalized_search_prefixes, live_logger
from qualia.utils.plugin_utils import get_orphan_node_ids, PluginUtils


@plugin
class Qualia(PluginDriver):
    def __init__(self, nvim: Nvim, ide_debugging: bool = False):
        try:
            live_logger.attach_nvim(nvim)
            bootstrap()
            super().__init__(nvim, ide_debugging)
        except Exception as e:
            live_logger.critical("Error during initialization" + exception_traceback(e))
            raise e

    @command("TriggerSync", sync=True, nargs='?')
    def trigger_sync_cmd(self, args: list = None) -> None:
        self.trigger_sync(True if args and int(args[0]) else False)

    @command("NavigateNode", sync=True, nargs='?')
    def navigate_cur_node(self, args: list[NodeId] = None) -> None:
        node_id = (args and args[0]) or self.line_info(self.current_line_number()).node_id
        with Database() as db:
            self.navigate_node(node_id, False, db)

    @command("HoistNode", sync=True)
    def hoist_node(self) -> None:
        line_num = self.current_line_number()
        view = self.line_node_view(line_num)
        with Database() as db:
            db.set_node_view(view, self.file_path_transposed(self.nvim.current.buffer.name))
            self.navigate_node(view.main_id, True, db)

    @command("ToggleQualia", sync=True)
    def toggle_parser(self) -> None:
        self.enabled = not self.enabled
        if self.enabled:
            self.trigger_sync(True)
        live_logger.info("Qualia enabled" if self.enabled else "Qualia paused")

    @command("PromoteNode", sync=True)  # TODO: Range and preserve view
    def promote_node(self) -> None:
        current_line_number = self.current_line_number()
        try:
            ancestory = self.view_node_path(current_line_number, 2)
            cur_line_info = ancestory[0]
            assert cur_line_info.nested_level >= 2
        except Exception as e:
            live_logger.error("Can't promote node beyond current level.")
            live_logger.critical(exception_traceback(e))
            return

        parent_line_info = ancestory[1]

        transposed = self.file_path_transposed(self.nvim.current.buffer.name)
        cur_node_id = cur_line_info.node_id
        parent_id = cur_line_info.parent_view.main_id
        grandparent_id = parent_line_info.parent_view.main_id

        with Database() as db:
            cur_node_siblings = db.get_node_descendants(parent_id, transposed, False)
            cur_node_siblings.remove(cur_node_id)
            db.set_node_descendants(parent_id, cur_node_siblings, transposed)

            parent_node_siblings_list = list(db.get_node_descendants(grandparent_id, transposed, False))
            parent_node_siblings_list.insert(parent_node_siblings_list.index(parent_id) + 1, cur_node_id)
            db.set_node_descendants(grandparent_id, OrderedSet(parent_node_siblings_list), transposed)

            # Preserve expanded state: restore view

        self.trigger_sync(True)

    @command("ToggleFold", sync=True, nargs='?')
    def toggle_fold(self, args: list[int] = None) -> None:
        cur_line_info = self.line_info(self.current_line_number())

        if cur_line_info is self.line_info(0):
            live_logger.error("Can't toggle top level node")
        else:
            cur_context = cur_line_info.parent_view.sub_tree
            assert cur_context is not None
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
            live_logger.error(f"Minimum fold level should be 1. Argument list provided: {args}")
        else:
            self.main(None, fold_level)

    @command("TransposeNode", sync=True, nargs='?')
    def transpose(self, args: list[str] = None) -> None:
        currently_transposed = self.file_path_transposed(self.nvim.current.buffer.name)
        node_id = self.line_info(self.current_line_number()).node_id
        try:
            replace_buffer = False if args and int(args[0]) else True
        except ValueError:
            live_logger.error(
                "Optional argument to specify buffer replacement should be 1 or 0. Provided argument list: ", args)
        else:
            with Database() as db:
                self.replace_with_file(self.node_id_filepath(node_id, not currently_transposed, db), replace_buffer)

    @command(PluginUtils.fzf_sink_command, nargs=1, sync=True)
    def fzf_sink(self, selections: list[str]):
        for i, selected in enumerate(selections):
            # FZF adds confusing backslash before delimiter, so idx - 1
            # `017b99da-b1b5-19e9-e98d-8584cf46cfcf\^Ilaskdjf`
            node_id = NodeId(selected[:selected.index(_FZF_LINE_DELIMITER) - 1])
            with Database() as db:
                node_filepath = self.node_id_filepath(node_id, False, db)
            if i == 0:
                self.replace_with_file(node_filepath, False)
            else:
                self.nvim.command(f"edit {node_filepath}")

    @command("SearchQualia", sync=True, nargs='*')
    def search_nodes(self, query_strings: list[str]) -> None:
        prefixes = normalized_search_prefixes(' '.join(query_strings))
        fzf_lines = matching_nodes_content(prefixes)
        self.fzf_run(fzf_lines, ' '.join(query_strings), False)

    @command("ListOrphans", sync=True)
    def list_orphans(self) -> None:
        with Database() as db:
            orphan_fzf_lines = [fzf_input_line(node_id, db.get_node_content_lines(node_id),
                                               False if db.get_node_descendants(node_id, True, True) else True)
                                for node_id in get_orphan_node_ids(db)]
        self.fzf_run(orphan_fzf_lines, '', True)

    @command("RemoveOrphans", sync=True, nargs='?')
    def remove_orphans(self, args: tuple[int] = tuple()) -> None:
        skip_confirm = args and int(args[0])
        if not (skip_confirm or self.nvim.funcs.confirm("Remove orphans?", "&Be kind\n&Yes", 1) == 2):
            return
        with Database() as db:
            for orphan_node_id in get_orphan_node_ids(db):
                db.delete_node(orphan_node_id)

    @function("CurrentNodeId", sync=True)
    def current_node_id(self, line_num: list[int]) -> NodeId:
        return self.line_info(line_num[0] if line_num else self.current_line_number()).node_id


if __name__ == "__main__":
    from qualia.utils.init_utils import setup_logger

    setup_logger()

    nvim_debug = attach('socket', path=NVIM_DEBUG_PIPE)  # path=environ['NVIM_LISTEN_ADDRESS'])
    qualia_debug = Qualia(nvim_debug, True)
    while True:
        try:
            if qualia_debug.should_continue(False):
                qualia_debug.main(None, None)
        except OSError as exp:
            raise exp

        sleep(0.01)
