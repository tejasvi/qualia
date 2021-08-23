from __future__ import annotations

import traceback
from os.path import basename
from pathlib import Path
from subprocess import check_call, CalledProcessError
from sys import executable, argv, setrecursionlimit, getrecursionlimit

from lmdb import Cursor
from pkg_resources import working_set

NVIM_PIPE = r'\\.\pipe\nvim-15600-0'

FZF_DELIMITER = "\t"


def install_dependencies() -> None:
    required = {'lmdb', 'orderedset', 'markdown-it-py', 'appdirs', 'base65536', 'pynvim', 'bloomfilter-py',
                'typing-extensions'}
    installed = {pkg.key for pkg in working_set}
    for package in required - installed:
        install_command = [executable, "-m", "pip", "install", package]
        try:
            check_call(install_command)
        except CalledProcessError as e:
            print("ERROR: Can't install the missing ", package, " dependency. Attempting ", ' '.join(install_command))
            raise e


install_dependencies()

from qualia.realtime import Realtime
from collections import defaultdict
from time import sleep, time
from typing import Any, Optional, cast

from pynvim import plugin, Nvim, function, attach, command, autocmd
from pynvim.api import Buffer

from qualia.config import DB_FOLDER, LEVEL_SPACES, ROOT_ID_KEY, APP_FOLDER_PATH, LOG_FILENAME
from qualia.git import sync_with_git
from qualia.models import DuplicateNodeException, NodeId, BufferNodeId, LastSeen, UncertainNodeChildrenException, \
 \
    Client, View, BufferId, LineInfo, Cursors
from qualia.render import render
from qualia.search import matching_nodes_content
from qualia.sync import sync_buffer
from qualia.utils import Database, put_key_val, get_uuid, set_client_if_new, name_to_node_id, get_key_val, \
    bootstrap, node_id_to_filepath, normalized_prefixes, buffer_inverted, logger, resolve_main_id

GIT_FLAG_ARG = "git"


@plugin
class Qualia:
    def __init__(self, nvim: Nvim, ide: bool = False):

        self.ide_debug = nvim.eval('v:servername') == NVIM_PIPE
        if self.ide_debug and not ide:
            exit()
        bootstrap()
        self.nvim = nvim
        self.realtime_session = Realtime()
        self.count = 0
        self.autocmd = None
        self.clone_ns = self.nvim.funcs.nvim_create_namespace("clones")
        self.duplicate_ns = self.nvim.funcs.nvim_create_namespace("duplicates")
        self.buffer_last_seen: dict[BufferId, LastSeen] = defaultdict(LastSeen)
        self.undo_seq: dict[BufferId, int] = {}
        self.changedtick: dict[BufferId, int] = defaultdict(lambda: -1)
        self.last_git_sync = 0

    def log(self, *args: Any):
        text = ' - '.join([str(text) for text in args])
        if self.autocmd:
            self.nvim.err_write(text + '\n')
        else:
            print(text)

    @autocmd("TextChanged,FocusGained,BufEnter,InsertLeave,BufLeave,BufFilePost", pattern='*.q.md', sync=True,
             allow_nested=False,
             eval=None)
    def auto_main(self, *_args) -> None:
        self.autocmd = True
        try:
            if not self.should_continue():
                return
            self.main(None, None)
        except Exception as e:
            self.nvim.err_write(
                "Something went wrong :(\n" + '\n'.join(traceback.format_exception(None, e, e.__traceback__)))

    @function("NavigateNode", sync=True)
    def navigate_node(self, node_id: Optional[NodeId] = None) -> None:
        if node_id is None:
            node_id = self.line_info(self.current_line_number()).node_id
        inverted = buffer_inverted(self.nvim.current.buffer.name)
        filepath = node_id_to_filepath(node_id, inverted)
        if self.nvim.current.buffer.name != filepath:
            self.replace_with_file(filepath)

    def replace_with_file(self, filepath) -> None:
        self.nvim.command(f"silent bdelete | silent edit! {filepath} | silent normal hl")

    @function("HoistNode", sync=True)
    def hoist_node(self, *_args) -> None:
        line_num = self.current_line_number()
        view = self.line_node_view(line_num)
        self.log(view)
        self.main(view, None)

    @function("Toggle", sync=True)
    def toggle(self, *_args, should_expand: Optional[bool] = None) -> None:
        cur_line_info = self.line_info(self.current_line_number())
        currently_expanded = cur_line_info.context[cur_line_info.node_id] is not None
        if should_expand is None:
            should_expand = not currently_expanded
        if currently_expanded != should_expand:
            cur_line_info.context[cur_line_info.node_id] = (
                    cur_line_info.context[cur_line_info.node_id] or {}) if should_expand else None
            view = self.line_node_view(0)
            self.main(view, None)

    @function("FoldN", sync=True)
    def fold_n(self, args: list) -> None:
        fold_level = args[0]
        assert fold_level > 0
        self.main(None, fold_level)

    @function("Invert", sync=True)
    def invert(self, *_args) -> None:
        node_id = self.line_info(self.current_line_number()).node_id
        self.replace_with_file(node_id_to_filepath(node_id, not buffer_inverted(self.nvim.current.buffer.name)))

    fzf_sink_function = "NodeFzfSink"

    @command(fzf_sink_function, nargs=1, sync=True)
    def fzf_sink(self, selections: list[str]):
        for i, selected in enumerate(selections):
            node_id = NodeId(selected[:selected.index(FZF_DELIMITER)])
            node_filepath = node_id_to_filepath(node_id, False)
            if i == 0:
                self.replace_with_file(node_filepath)
            else:
                self.nvim.command(f"edit {node_filepath}")

    @function("SearchNodes", sync=True)
    def search_nodes(self, query_strings: list[str]) -> None:
        prefixes = normalized_prefixes(' '.join(query_strings))
        fzf_lines = matching_nodes_content(prefixes)
        self.log(query_strings, prefixes, fzf_lines)
        self.nvim.call("fzf#run",
                       {'source': fzf_lines, 'sink': self.fzf_sink_function, 'window': {'width': 0.9, 'height': 0.9},
                        'options': ['--delimiter', FZF_DELIMITER, '--with-nth', '2..']})  # , '--nth', '2..'

    def main(self, view: Optional[View], fold_level: Optional[int]) -> None:
        t = time()
        trigger_buffer: Buffer = self.nvim.current.buffer
        self.delete_highlights(trigger_buffer.number)

        with Database() as cursors:
            inverted, main_id = self.navigate_filepath(trigger_buffer.name, cursors, view)

            current_buffer = self.nvim.current.buffer
            buffer_id = self.current_buffer_id()
            if buffer_id is None:
                return
            last_seen = self.buffer_last_seen[buffer_id]

            initial_time = time()
            initial_time_diff = time() - t

            try:
                while True:
                    try:
                        root_view = view or sync_buffer(list(current_buffer), main_id, last_seen, cursors,
                                                        inverted, self.realtime_session)
                        sync_time = time()
                        sync_time_diff = time() - initial_time
                        break
                    except RecursionError:
                        if self.nvim.funcs.confirm("Too many nodes open. Expect crash on weak hardware. Continue?",
                                                   "&No\n&Yes", 1) == 1:
                            return
                        setrecursionlimit(getrecursionlimit() * 2)
            except DuplicateNodeException as exp:
                self.handle_duplicate_node(current_buffer, exp)
            except UncertainNodeChildrenException as exp:
                self.handle_uncertain_node_children(current_buffer, exp, main_id, last_seen)
            else:
                self.buffer_last_seen[buffer_id] = render(root_view, current_buffer, self.nvim, cursors, inverted,
                                                          fold_level)
                l = time() - t
                if l > 0.1:
                    self.log("TOOK", l, initial_time_diff, sync_time_diff, time() - sync_time)
                self.nvim.command("silent set write | silent update")
                self.changedtick[buffer_id] = self.nvim.eval("b:changedtick")

        # sync_with_git()
        # if time() - self.last_git_sync > 15:
        #     Popen([executable, __file__, GIT_FLAG_ARG], start_new_session=True)
        #     self.last_git_sync = time()

    def navigate_filepath(self, buffer_name: str, cursors: Cursors, view) -> tuple[bool, NodeId]:
        try:
            main_id, inverted = resolve_main_id(buffer_name, cursors.content)
        except ValueError:
            main_id, inverted = self.navigate_root_node(buffer_name, cursors.metadata)
        if view and main_id != view.main_id:
            self.navigate_node(view.main_id)
        return inverted, main_id

    def navigate_root_node(self, buffer_name: str, metadata_cursor: Cursor) -> tuple[NodeId, bool]:
        inverted = buffer_inverted(basename(buffer_name))
        root_id = cast(NodeId, get_key_val(ROOT_ID_KEY, metadata_cursor))
        self.replace_with_file(node_id_to_filepath(root_id, inverted))
        self.log("Redirecting to root node")
        return root_id, inverted

    def line_node_view(self, line_num) -> View:
        line_info = self.line_info(line_num)
        node_id = line_info.node_id
        view = View(node_id, line_info.context[node_id])
        return view

    def line_info(self, line_num) -> LineInfo:
        line_data = self.buffer_last_seen[self.current_buffer_id()].line_info
        if line_data is not None:
            for line_number in range(line_num, -1, -1):
                if line_number in line_data:
                    return line_data[line_number]
        raise Exception("Current line info not found " + str(line_data))

    def poll(self, *_args) -> None:
        self.autocmd = False
        if not self.should_continue():
            return
        self.main(None, None)

    def delete_highlights(self, buffer_numer) -> None:
        for ns_id in (self.clone_ns, self.duplicate_ns):
            self.nvim.funcs.nvim_buf_clear_namespace(buffer_numer, ns_id, 0, -1)

    def should_continue(self) -> bool:
        if self.nvim.funcs.mode().startswith("i"):
            return False

        undotree = self.nvim.funcs.undotree()

        if self.ide_debug and undotree["synced"] == 0:
            return False

        if undotree["seq_cur"] < undotree["seq_last"]:
            return False

        buffer_id = self.current_buffer_id()
        if buffer_id is None:
            return False
        if buffer_id in self.undo_seq and undotree["seq_cur"] - self.undo_seq[buffer_id] > 1:
            self.buffer_last_seen.pop(buffer_id)
        self.undo_seq[buffer_id] = undotree["seq_cur"]

        # Undo changes changedtick so check that before
        changedtick = self.nvim.eval("b:changedtick")
        if changedtick == self.changedtick[buffer_id]:
            return False
        else:
            self.changedtick[buffer_id] = changedtick

        if not self.nvim.current.buffer.name.endswith(".q.md"):
            return False

        return True

    def handle_uncertain_node_children(self, buffer: Buffer, exp: UncertainNodeChildrenException, main_id: NodeId,
                                       last_seen: LastSeen):
        self.nvim.command("set nowrite")
        self.log(f"Node children uncertain. Manual save required, {exp.node_id}, {exp.line_range}")
        for line_num in range(exp.line_range[0], min(exp.line_range[1], exp.line_range[0] + 50)):
            self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.clone_ns, "ErrorMsg", line_num, 0, -1)
        if self.nvim.funcs.confirm("Current state is uncertain.", "&Skip parsing\n&Force parse", 1) == 2:
            # TODO: Replace below with saner version. Currently  below resets the view.
            last_seen.clear_except_main(main_id)
            self.main(None, None)

    def handle_duplicate_node(self, buffer: Buffer, exp: DuplicateNodeException):
        self.nvim.command("set nowrite")
        self.log(f"Unsynced duplicate, {exp.node_id}, {exp.line_ranges}")
        for node_locs in exp.line_ranges:
            for line_num in range(node_locs[0], node_locs[1]):
                self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.clone_ns, "ErrorMsg", line_num, 0, -1)

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


if __name__ == "__main__":
    if len(argv) >= 2 and argv[1] == GIT_FLAG_ARG:
        sync_with_git()
    else:
        snvim = attach('socket', path=NVIM_PIPE)  # path=environ['NVIM_LISTEN_ADDRESS'])
        q = Qualia(snvim, True)
        while True:
            q.poll()
            sleep(0.01)
        # main()

"""
import lmdb
from lmdb.tool import dump_cursor_to_fp, restore_cursor_from_fp

import shutil

shutil.rmtree("test", ignore_errors=True)

BUF_SIZE = 10485760

env = lmdb.open("test")
db = env.open_db(None)

with env.begin(write=True) as txn:
    txn.put("key1".encode(), "first_line\nsecond_line".encode())
    txn.put("key2".encode(), "first_line\nsecond_line".encode())

with open("dumpf", "rb", BUF_SIZE) as fp:
    with env.begin(buffers=True, write=True) as txn:
        restore_cursor_from_fp(txn, fp, db)

with open("dumpf", "wb", BUF_SIZE) as fp:
    with env.begin(buffers=True) as txn:
        dump_cursor_to_fp(txn.cursor(), fp)

env.close()

exit()

from pynvim import attach
from uuid import uuid4
from base64 import urlsafe_b64encode
from functools import partial


# def decode_uuid(encoded):
#     return str(UUID(bytes=urlsafe_b64decode(encoded)))

# with open("data", "w") as f:
#     f.writelines(
#         [
#             x + "\n"
#             for x in [
#                 " ".join([get_uuid() for _ in range(4)]),
#                 " First line\n Continue it",
#                 " ".join([get_uuid() for _ in range(3)]),
#                 " Second line",
#                 " ".join([get_uuid() for _ in range(5)]),
#                 " Third line",
#             ]
#         ]
#     )
# exit()


def oset(it):
    return {x: None for x in it}


def parse(get_line, test=False):
    data = {}
    uids = get_line()
    while True:
        uid, out_uids, in_uids = uids.rstrip("\n").split("  ")
        if uid == "":
            break
        content = []
        while True:
            line = get_line().rstrip("\n")
            if line and line[0] == " ":
                content.append(line)
            else:
                uids = line
                break
        if test:
            assert content
            assert uid not in data
            assert len(uid) == 22 and all([len(x) == 22 for x in out_uids])
        data[uid] = {
            "content": content,
            "out": oset(out_uids.split(" ")),
            "in": oset(in_uids.split(" ")),
        }
    return data


TEST = True
if TEST:

    def f(counter, data):
        idx = counter[0]
        if idx >= len(data):
            return ""
        counter[0] += 1
        return data[idx]


    def test(
            res,
            args,
    ):
        try:
            assert res == parse(partial(f, [0], args))
        except res:
            pass


    uids = [get_uuid() for _ in range(10)]

    test(
        {uids[0]: {"content": [" Test", " Second line", "  Third line"], "out": {}}},
        [uids[0], " Test", " Second line", "  Third line"],
    )
    test(AssertionError, [uids[0][:21], " UUID not 22 long"])
    test(AssertionError, [uids[0], "No prefix space in content"])
    test(AssertionError, [uids[0] + " ", " Trailing space in UIDs"])
    test(AssertionError, [" " + uids[0], " UID starting with space"])
    test(
        {
            uids[0]: {"content": [" Test"], "out": oset(uids[1:2])},
            uids[2]: {"content": [" Test2"], "out": oset(uids[3:5])},
        },
        [" ".join(uids[:2]), " Test", " ".join(uids[2:5]), " Test2"],
    )
    test(
        AssertionError,
        [" ".join(uids[:2]), " Duplicate node ID", " ".join(uids[:3]), " Test"],
    )

with open("data") as f:
    data = parse(f.readline)

if not data:
    raise Exception

max_level = 10


def render_lines(data, nvim_buffer):
    root = next(iter(data.keys()))
    stack = [(root, 0)]
    buffer_lines = []
    while stack:
        node, level = stack.pop()
        content = data[node]["content"]

        space_padding = " " * level * 4
        buffer_lines.append(space_padding + "*" + content[0])
        buffer_lines += [space_padding + " " + line for line in content[1:]]

        if level < max_level:
            stack.extend([(c, level + 1) for c in reversed(data[node]["out"].keys())])
    nvim.current.buffer[:] = render_lines(data)


nvim = attach("socket", path=r"\\." "\\" r"pipe\nvim-15176-0")  # "/tmp/nvim")
call = nvim.call

exit()

current_cursor = nvim.current.window.cursor

call("setloclist", 0, [{"lnum": 2, "col": 1, "text": "haha"}])
loclist = call("getloclist")

mark_ns = call("nvim_create_namespace", "qualia")
mark_id = call("nvim_buf_set_extmark", 0, mark_ns, 0, 0, {})

call("nvim_buf_get_extmark_by_id", 0, mark_ns, mark_id, {})
call("nvim_buf_get_extmarks", 0, mark_ns, 0, -1, {})

# from ptpython import embed

# embed(globals(), locals())

"""

"""
TODO:
Max level limit
While placing nodes on buffer, order by nodeID

Buffer opened

* Saving buffer changes
* Loading a tree
For each view create a new _file_ (store in tmp?) and vim will remember marks, jump locations etc for that file (in its cache location).
After switching to different view, vim will reopen the last _view_ file when going back.
How it will work for VS Code?

Why content hash check with db is faulty?
User changes a node in buffer. Before contents are synced
    User uses a different instance, changes the node content equal to last_seen node content in previous instance.
    When previous instance is synced, nothing amiss is found and latest user changes are overwritten due to latest db write policy
    To fix, last_seen state has last seen version number of db and if during sync db gives larger number, the db content is newer and conflicts are handled 
    accordingly (if the buffer has newer content as well).

TextChanged: In normal mode and on leaving insert mode
    Else sync every 5 seconds? if stayed in insert mode for long.
When conflict with the children, create a link to the node from the _child conflict list_ node.
    Can be done the same with content conflict.
    
Do something like React while refilling buffer.
    Hash all lines in buffer and lines to be filled in buffer. Do minimal replacements to not disturb position too much
    
https://github.com/jacobsimpson/nvim-example-python-plugin
comparing content hash is enough to ensure no conflict since history hash (like blockchain) gives no additional beneft except the indication that node went through different modifications to have the identical content. Since previous content is not stored, that information is largely useless.
node-id points to content, children-hash since realtime syncing will be read heavy and reads are cheap with lmdb.
to have the version history in the db, each node is the sub-database with incrementing counter as keys. Last key gives the latest revision's content and children hash.
    The obtained hash is indexed on the main database. (they can be distinguished from node_id which are 128 bits and they are 256 bits (sha2))
    There is overhead of subdatabase
        Some way to emulate it in top level database? E.g. version hash pointing to previous version hash.
        
Most of timethere is not conflict
    Conflict arises when
        The currently edited node is present elsewhere on the tree
        Two sources are editing the same node

There is main two-way _sync_ function. Like GC pauses.

Link every new node from a global index node. No one is orphan anymore. How to handle rendering in this case?
LastSeen is needed to store the last render state since the DB can change between the renders and then next sync with overwrite the external changes instead of detecting conflicts using last_seen da
From directory containing vimrc with: let &runtimepath.=','.escape(expand('<sfile>:p:h'), '\,')
    nvim -u vimrc and then UpdateRemotePlugins
    Then start nvim normally from anywhere and open file with .q extension
For pycharm debugging, nvim --listen \\.\pipe\nvim-15600-0 filename
For normal run, nvim -u vimrc and :UpdateRemotePlugins
During render keep track of last number used for buffer id. To get new id, increment it by one convert to bytes and then into base65536
Live cloud sync aggressively if other client is checked fetched content from cloud recently. Else delay the sync.
    Each client DB has an ID
    Each client on fetch will set its DB ID
    The other client can decide the running average of sync delay by checking if some other client DB "checked" cloud for new updates recently based on last modification time
Git syncing: top level directories with note ids as their name (hex encoded to be case agnostic).
    Each directory contains README.md containing the content of that node
    The contained symlinks to directories on root level are children.
    The system is flexible to include arbitrary content in a node like attachments.
        When there is single non child symlink it represents _the_ content of the node.
subprocess.Popen to trigger backup/git sync in independant process
Manual override save works by  saving the buffer as is  and clearing the last_seen 
TODO: Merge conflicting view while syncing with git
Git:
    While true:
        Pull from remote
        For each node in the repo, merge and handle conflict with the db
        Clear repo directory
        Starting from root node in db (first node), visit its descendants and add them to the repo.
        Commit
        If push success
            break
        else:
            reset to origin: git reset --hard origin
            startover
Update backlinks only in the local db
For search, create bloom (or cuckoo) filter for each node.
    Tokenization
        split content, filter the spaces `re.split('(\W)', 'a-b ter.ce-ret 000-123')`
            Remove spaces
            Limit only to max first three characters
                Then pipe the node content to fzf
firestore for realtime
git for syncing
TODO:
    :call setline('.', substitute(getline('.'), '\%2c.', 'a', ''))
    Useful when creating new node and prevent odd cursor movement.
        Linelevel difflib
"""
