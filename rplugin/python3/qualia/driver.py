from __future__ import annotations

from sys import setrecursionlimit, getrecursionlimit
from threading import Thread
from time import time
from typing import Optional

from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import DEBUG
from qualia.git import sync_with_git
from qualia.models import View, DuplicateNodeException, UncertainNodeChildrenException
from qualia.realtime import Realtime
from qualia.render import render
from qualia.sync import sync_buffer
from qualia.utils.common_utils import Database
from qualia.utils.nvim_utils import NvimUtils


class NvimDriver(NvimUtils):
    def __init__(self, nvim: Nvim, debugging: bool):
        super().__init__(nvim, debugging)
        self.realtime_session = Realtime(lambda: self.main(None, None))

    def main(self, view: Optional[View], fold_level: Optional[int]) -> None:
        t0 = time()
        current_buffer: Buffer = self.nvim.current.buffer

        with Database() as cursors:
            switched_buffer, transposed, main_id = self.navigate_filepath(current_buffer.name, cursors, view)
            if switched_buffer:
                return

            buffer_id = self.current_buffer_id()
            if buffer_id is None:
                return
            last_seen = self.buffer_last_seen[buffer_id]

            t1 = time()
            del1 = time() - t0

            while True:
                try:
                    while True:
                        try:
                            root_view = view or sync_buffer(list(current_buffer), main_id, last_seen, cursors,
                                                            transposed, self.realtime_session)
                            break
                        except RecursionError:
                            if self.nvim.funcs.confirm("Too many nodes open. Expect crash on slow hardware. Continue?",
                                                       "&No\n&Yes", 1) == 1:
                                return
                            setrecursionlimit(getrecursionlimit() * 2)
                        finally:
                            t2 = time()
                            del2 = time() - t1
                except DuplicateNodeException as exp:
                    self.handle_duplicate_node(current_buffer, exp)
                except UncertainNodeChildrenException as exp:
                    if self.handle_uncertain_node_children(current_buffer, exp, last_seen):
                        continue
                else:
                    self.delete_highlights(current_buffer.number)
                    self.buffer_last_seen[buffer_id] = render(root_view, current_buffer, self.nvim, cursors, transposed,
                                                              fold_level)
                break

        total = time() - t0
        if DEBUG and total > 0.1:
            self.print_message("Took: ", ' '.join([str(round(n, 3)) for n in (total, del1, del2, time() - t2)]))

        self.nvim.command("silent set write | silent update")
        self.changedtick[buffer_id] = self.nvim.eval("b:changedtick")

        if DEBUG or time() - self.last_git_sync > 15:
            Thread(target=sync_with_git, name="SyncGit").start()
            self.last_git_sync = time()


r"""
Why content hash check with db is faulty?
    User changes a node in buffer. Before the buffer contents are synced,
        User uses a different instance, changes the node content equal to last_seen node content in previous instance.
        When previous instance is synced, nothing amiss is found and latest user changes are overwritten due to latest db write policy
        To fix, last_seen state has last seen version number of db and if during sync db gives larger number, the db content is newer and conflicts are handled accordingly (if the buffer has newer content as well).

https://github.com/jacobsimpson/nvim-example-python-plugin

LastSeen is needed to store the last render state since the DB can change between the renders and then next sync with overwrite the external changes instead of detecting conflicts using last_seen da

From directory containing vimrc with: let &runtimepath.=','.escape(expand('<sfile>:p:h'), '\,')
nvim -u vimrc and then UpdateRemotePlugins (every time commands change)
Then start nvim normally from anywhere and open file with .q.md extension

For pycharm debugging, nvim --listen \\.\pipe\nvim-15600-0 filename

:call setline('.', substitute(getline('.'), '\%2c.', 'a', '')) TODO
    Useful when creating new node and prevent odd cursor movement.
        Linelevel difflib
"""
