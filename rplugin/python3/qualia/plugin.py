from time import sleep
from typing import Optional, Type

from orderedset import OrderedSet
from pynvim import plugin, Nvim, command, attach, function

from qualia.config import _FZF_LINE_DELIMITER, NVIM_DEBUG_PIPE
from qualia.database import Database, MuDatabase
from qualia.driver import PluginDriver
from qualia.models import NodeId, MinimalDb, MutableDb, View, SourceId
from qualia.services.file_db import FileDb
from qualia.services.listener import RpcListenExternal
from qualia.services.search import matching_nodes_content, fzf_input_line
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import exception_traceback, normalized_search_prefixes, live_logger, StartLoggedThread
from qualia.utils.plugin_utils import get_orphan_node_ids, PluginUtils

Sources: dict[str, Type[MinimalDb]] = {'directory': FileDb}


@plugin
class Qualia(PluginDriver):
    def __init__(self, nvim: Nvim, ide_debugging: bool = False):
        try:
            live_logger.attach_nvim(nvim)
            StartLoggedThread(target=RpcListenExternal, name="RPClistener", delay_seconds=2)
            bootstrap()
            super().__init__(nvim, ide_debugging)
        except Exception as e:
            live_logger.critical("Error during initialization" + exception_traceback(e))
            raise e

    @command("TriggerSync", sync=True, nargs='?')
    def trigger_sync_cmd(self, args: list = None) -> None:
        self.trigger_sync(True if args and int(args[0]) else False)

    @command("NavigateNode", sync=True, nargs='*')
    def navigate_cur_node(self, args: list[NodeId] = None) -> None:
        view = self.line_node_view(self.current_line_number())
        view.sub_tree = None

        if args:
            view.node_id = args[0]
            if len(args) == 2:
                view.source_id = args[1]
            else:
                live_logger.critical("The number of arguments should be less than equal to two. First optional argument is node ID and optional second is source ID.")
                return

        self.navigate_node(view, False, db)

    @command("HoistNode", sync=True)
    def hoist_node(self) -> None:
        line_num = self.current_line_number()
        view = self.line_node_view(line_num)
        with Database() as db:
            if isinstance(db, MutableDb):
                db.set_node_view(view)
        self.navigate_node(view, True, db)

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
            if isinstance(db, MutableDb):
                cur_node_siblings = db.get_node_descendants(parent_id, transposed, False, temporary)
                cur_node_siblings.remove(cur_node_id)
                db.set_node_descendants(parent_id, cur_node_siblings, transposed)

                parent_node_siblings_list = list(db.get_node_descendants(grandparent_id, transposed, False, temporary))
                parent_node_siblings_list.insert(parent_node_siblings_list.index(parent_id) + 1, cur_node_id)
                db.set_node_descendants(grandparent_id, OrderedSet(parent_node_siblings_list), transposed)
            else:
                PluginUtils.unmutable_db_error()

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
        try:
            replace_buffer = False if args and int(args[0]) else True
        except ValueError:
            live_logger.error(
                "Optional argument to specify buffer replacement should be 1 or 0. Provided argument list: ", args)
        else:
            cur_view = self.line_node_view(self.current_line_number())
            cur_view.transposed = not cur_view.transposed
            self.replace_with_file(self.node_id_filepath(cur_view, db), replace_buffer)

    @command(PluginUtils.fzf_sink_command, nargs=1, sync=True)
    def fzf_sink(self, selections: list[str]):
        for i, selected in enumerate(selections):
            # FZF adds confusing backslash before delimiter, so idx - 1
            # `017b99da-b1b5-19e9-e98d-8584cf46cfcf\^Ilaskdjf`
            first_delimiter_idx = selected.index(_FZF_LINE_DELIMITER) - 1
            node_id = NodeId(selected[:first_delimiter_idx])
            source_id = SourceId(selected[first_delimiter_idx:selected.index(_FZF_LINE_DELIMITER,first_delimiter_idx) - 1])
            node_filepath = self.node_id_filepath(View(node_id, source_id, None, False), db)
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
            orphan_fzf_lines = [fzf_input_line(node_id, db.get_node_content_lines(node_id, temporary),
                                               False if db.get_node_descendants(node_id, True, True, temporary) else True)
                                for node_id in get_orphan_node_ids(db)]
        self.fzf_run(orphan_fzf_lines, '', True)

    @command("RemoveOrphans", sync=True, nargs='?')
    def remove_orphans(self, args: tuple[int] = tuple()) -> None:
        skip_confirm = args and int(args[0])
        if not (skip_confirm or self.nvim.funcs.confirm("Remove orphans?", "&Be kind\n&Yes", 1) == 2):
            return
        with Database() as db:
            if isinstance(db, MutableDb):
                for orphan_node_id in get_orphan_node_ids(db):
                    db.delete_node(orphan_node_id)
            else:
                PluginUtils.unmutable_db_error()

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
