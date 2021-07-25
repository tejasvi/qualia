import shutil
import traceback
from collections import defaultdict
from subprocess import CalledProcessError
from subprocess import check_call
from sys import executable
from time import sleep
from typing import Any

from pkg_resources import working_set
from pynvim import plugin, Nvim, function, attach, autocmd

from qualia import states
from qualia.config import DB_FOLDER, LEVEL_SPACES
from qualia.models import DuplicateException, NodeId, BufferNodeId, Ledger, CloneChildrenException
from qualia.render import render
from qualia.sync import sync_buffer
from qualia.utils import Database

shutil.rmtree(DB_FOLDER, ignore_errors=True)


def install_dependencies() -> None:
    required = {'lmdb', 'orderedset', 'markdown-it-py'}
    installed = {pkg.key for pkg in working_set}
    for package in required - installed:
        command = [executable, "-m", "pip", "install", package]
        try:
            check_call(command)
        except CalledProcessError as e:
            print("ERROR: Can't install the missing ", package, " dependency. Attempting ", ' '.join(command))
            raise e


install_dependencies()


@plugin
class Qualia:
    def __init__(self, nvim: Nvim):
        self.nvim = nvim
        self.count = 0
        self._changedtick = None
        self._undo_seq = self.nvim.funcs.undotree()["seq_last"]
        self.autocmd = None
        self.clone_ns = self.nvim.funcs.nvim_create_namespace("clones")
        self.duplicate_ns = self.nvim.funcs.nvim_create_namespace("duplicates")
        self.ledgers: dict[int, Ledger] = defaultdict(Ledger)

    def log(self, *args: Any):
        text = ' - '.join([str(text) for text in args])
        if self.autocmd:
            self.nvim.err_write(text + '\n')
        else:
            print(text)

    @autocmd("TextChanged,FocusGained,BufEnter,InsertLeavePre,BufLeave", pattern='*.q.md', sync=True,
             allow_nested=False,
             eval=None)
    def auto_main(self, *_args) -> None:
        self.autocmd = True
        try:
            self.main(*_args)
        except Exception as e:
            self.nvim.err_write(''.join(traceback.format_exception(None, e, e.__traceback__)))

    def poll(self, *_args) -> None:
        self.autocmd = False
        undotree = self.nvim.funcs.undotree()
        if undotree["seq_cur"] < undotree["seq_last"] or undotree["synced"] == 0:
            self.log(("UNDO RET", undotree["synced"] == 0, undotree["seq_cur"] < undotree["seq_last"]))
            return

        if self.nvim.funcs.mode().startswith("i"):
            return

        self.main()

    def delete_highlights(self, buffer_numer) -> None:
        for ns_id in (self.clone_ns, self.duplicate_ns):
            self.nvim.funcs.nvim_buf_clear_namespace(buffer_numer, ns_id, 0, -1)

    def should_continue(self) -> bool:
        buffer = self.nvim.current.buffer
        undotree = self.nvim.funcs.undotree()
        if undotree["seq_cur"] < undotree["seq_last"]:
            self.log(("UNDO RET", undotree["synced"] == 0, undotree["seq_cur"] < undotree["seq_last"]))
            return False
        if undotree["seq_cur"] - self._undo_seq > 1:
            self.ledgers.pop(buffer.number)
        self._undo_seq = undotree["seq_cur"]

        # Undo changes changedtick so check that before
        changedtick = self.nvim.eval("b:changedtick")
        if changedtick == self._changedtick:
            return False
        else:
            self._changedtick = changedtick

        return True

    def main(self, *_args) -> None:
        if not self.should_continue():
            return

        with Database() as cursors:
            buffer = self.nvim.current.buffer
            self.delete_highlights(buffer.number)
            states.ledger = self.ledgers[buffer.number]
            states.cursors = cursors
            try:
                root_view = sync_buffer(buffer, states.ledger)
                render(root_view, buffer, self.nvim, states.ledger, cursors)
                # write to filename with hex-encoded root ID
                self.nvim.command("set write")
            except DuplicateException as exp:
                self.nvim.command("set nowrite")
                self.log(f"Unsynced duplicate, {exp.node_id}, {exp.line_ranges}")
                for node_locs in exp.line_ranges:
                    for line_num in range(node_locs[0], node_locs[1]):
                        self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.clone_ns, "ErrorMsg", line_num, 0,
                                                               -1)
            except CloneChildrenException as exp:
                self.nvim.command("set nowrite")
                self.log(f"Radical organization. Manual save required, {exp.node_id}, {exp.line_range}")
                for line_num in range(exp.line_range[0], exp.line_range[1]):
                    self.nvim.funcs.nvim_buf_add_highlight(buffer.number, self.clone_ns, "ErrorMsg", line_num, 0, -1)

    @function("TestFunction")
    def test_function(self, *_args: Any):
        self.nvim.current.line = "Hello from your plugin!"


if __name__ == "__main__":
    snvim = attach('socket', path=r'\\.\pipe\nvim-15600-0')  # path=environ['NVIM_LISTEN_ADDRESS'])
    q = Qualia(snvim)
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
    User uses a different instance, changes the node content equal to ledger node content in previous instance.
    When previous instance is synced, nothing amiss is found and latest user changes are overwritten due to latest db write policy
    To fix, ledger state has last seen version number of db and if during sync db gives larger number, the db content is newer and conflicts are handled 
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
Ledger is needed to store the last render state since the DB can change between the renders and then next sync with overwrite the external changes instead of detecting conflicts using ledger da
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
Manual override save works by  saving the buffer as is  and clearing the ledger 
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
"""