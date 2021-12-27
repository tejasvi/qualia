from __future__ import annotations

from sys import setrecursionlimit, getrecursionlimit
from threading import Lock
from time import time
from typing import Optional, cast, TYPE_CHECKING

from qualia.config import DEBUG
from qualia.database import Database
from qualia.models import View, DuplicateNodeException, UncertainNodeChildrenException, Li
from qualia.render import render
from qualia.services.git import sync_with_git
from qualia.services.realtime import Realtime
from qualia.services.utils.service_utils import get_task_firing_event
from qualia.sync import sync_buffer
from qualia.utils.common_utils import live_logger, trigger_buffer_change, exception_traceback
from qualia.utils.plugin_utils import PluginUtils

if TYPE_CHECKING:
    from pynvim import Nvim
    from pynvim.api import Buffer


class PluginDriver(PluginUtils):
    def __init__(self, nvim, ide_debugging):
        # type: (Nvim, bool)->None
        super().__init__(nvim, ide_debugging)
        self.realtime_session = Realtime(lambda: trigger_buffer_change(nvim))
        self.sync_render_lock = Lock()
        self.git_sync_event = get_task_firing_event(lambda: sync_with_git(nvim), 1)  # 15)

    def main(self, root_view: Optional[View], fold_level: Optional[int]) -> None:
        t0 = time()
        with self.sync_render_lock:
            current_buffer: Buffer = self.nvim.current.buffer
            current_buffer_id = self.current_buffer_id()
            if current_buffer_id is None:
                return

            with Database() as db:
                try:
                    buffer_file_path = self.current_buffer_file_path()
                except AssertionError:
                    return
                switched_buffer, transposed, main_id = (
                    self.process_filepath(buffer_file_path, db) if root_view is None else self.process_view(root_view,
                                                                                                            db))
                if switched_buffer:
                    return

                last_sync = self.buffer_last_sync[current_buffer_id]

                t1 = time()
                del1 = t1 - t0

                while True:
                    try:
                        if root_view:
                            db.set_node_view(root_view, self.file_path_transposed(self.current_buffer_file_path()))
                        else:
                            while True:
                                try:
                                    buffer_lines = cast(Li, list(current_buffer))
                                    root_view = sync_buffer(buffer_lines, main_id, last_sync, db, transposed,
                                                            self.realtime_session, self.git_sync_event)
                                    break
                                except RecursionError:
                                    if self.nvim.funcs.confirm("Too many nodes may lead to crash on slow hardware.",
                                                               "&Pause parsing\n&Continue", 1) == 1:
                                        self.enabled = False
                                        return
                                    setrecursionlimit(getrecursionlimit() * 2)
                    except DuplicateNodeException as exp:
                        self.handle_duplicate_node(current_buffer, exp)
                    except UncertainNodeChildrenException as exp:
                        if self.handle_uncertain_node_descendant(current_buffer, exp, last_sync):
                            continue
                    else:
                        t2 = time()
                        del2 = t2 - t1
                        self.delete_highlights(current_buffer.number)
                        self.buffer_last_sync[current_buffer_id] = render(root_view, current_buffer, self.nvim, db,
                                                                  transposed, fold_level)
                        print(f"Rendered at {time()}s")

                        total = time() - t0
                        if DEBUG:  # and total > 0.1:
                            live_logger.debug(
                                "Took: " + ' '.join([str(round(n, 3)) for n in (total, del1, del2, time() - t2)]))
                    break

            # Might give OSError while debugging with Pycharm
            self.changedtick[current_buffer_id] = self.nvim.command_output("silent set write | silent update | echo b:changedtick")
            # self.nvim.command(
            #     "echom 'modified' getbufinfo(bufnr())[0].changed bufname() getbufline(bufnr(), 1, '$') b:changedtick | silent set write | silent update")

    def trigger_sync(self, force: bool) -> None:
        live_logger.debug(f"Trigger {time()}")
        if self.ide_debugging or not self.should_continue(force):
            return
        try:
            self.main(None, None)
        except Exception as e:
            self.nvim.err_write(
                "\nSomething went wrong :(\n\n" + exception_traceback(e))


r"""
Why content hash check with db is faulty?
    User changes a node in buffer. Before the buffer contents are synced,
        User uses a different instance, changes the node content equal to last_sync node content in previous instance.
        When previous instance is synced, nothing amiss is found and latest user changes are overwritten due to latest db write policy
        To fix, last_sync state has last seen version number of db and if during sync db gives larger number, the db content is newer and conflicts are handled accordingly (if the buffer has newer content as well).

https://github.com/jacobsimpson/nvim-example-python-plugin

LastSync is needed to store the last render state since the DB can change between the renders and then next sync with overwrite the external changes instead of detecting conflicts using last_sync da

From directory containing vimrc with: let &runtimepath.=','.escape(expand('<sfile>:p:h'), '\,')
nvim -u vimrc and then UpdateRemotePlugins (every time commands change)
Then start nvim normally from anywhere and open file with .q.md extension

For pycharm debugging, nvim --listen \\.\pipe\nvim-15600-0 filename

:call setline('.', substitute(getline('.'), '\%2c.', 'a', '')) TODO
    Useful when creating new node and prevent odd value_cursor movement.
        Linelevel difflib
"""
