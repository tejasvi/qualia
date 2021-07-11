import shutil
from base64 import urlsafe_b64encode
from collections import defaultdict
# from difflib import Differ
from dataclasses import dataclass
from hashlib import sha256
from json import dumps, loads
from os import environ
from re import compile
from secrets import token_urlsafe
from time import time_ns
from typing import Tuple, Union, Callable, NewType, cast, Any, FrozenSet
from uuid import uuid4

import lmdb
from commonmark import Parser
from pynvim import attach

DB_FILE = "test"

shutil.rmtree("test", ignore_errors=True)

CONFLICTS: str = "conflicts"

NodeId = NewType("NodeId", str)
BufferNodeId = NewType("BufferNodeId", str)
ContentRev = NewType("ContentRev", str)
ChildrenRev = NewType("ChildrenRev", str)
View = NewType("View", tuple[NodeId, dict])


def get_uuid():
    return urlsafe_b64encode(uuid4().bytes).rstrip(b"=").decode("ascii")


def get_time_uuid():
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    return urlsafe_b64encode(left_padded_time).decode() + token_urlsafe(10)


get_node_id: Callable[[], NodeId] = get_time_uuid
get_md_ast = Parser().parse


@dataclass
class Node:
    node_id: NodeId
    content_lines: list[str]
    children_ids: set[NodeId]


def sync(buffer_lines: list[str]):
    root_view = process_text(buffer_lines)
    save_nodes_to_db(cur_state.changed_nodes, root_view)


LEVEL_SPACES = 4
EXPANDED_BULLET = '* '
COLLAPSED_BULLET = '+ '


def content_lines_to_buffer_lines(content_lines: list[str], node_id: NodeId, level: int, expanded: bool) -> list[str]:
    ledger.node_buffer_id_map[node_id] = buffer_id = cast(BufferNodeId, node_id)
    space_count = LEVEL_SPACES * (level - 1) + 2
    space_prefix = ' ' * space_count
    buffer_lines = [
        space_prefix[:-2] + f"{EXPANDED_BULLET if expanded else COLLAPSED_BULLET}<!-- {buffer_id} --> " + content_lines[
            0]]
    for idx, line in enumerate(content_lines[1:]):
        buffer_lines.append(space_prefix + line)
    return buffer_lines


def render_buffer_lines(buffer_lines: list[str]):
    nvim = attach('socket', path=environ['NVIM_LISTEN_ADDRESS'])
    nvim.current.buffer[:] = buffer_lines


def restore_view(view: View):
    buffer_lines = get_buffer_lines_from_view(view)
    render_buffer_lines(buffer_lines)


def get_buffer_lines_from_view(view) -> list[str]:
    with lmdb.open(DB_FILE) as env:
        with env.begin() as txn:
            content_cur = txn.cursor(env.open_db("content", txn))
            children_cur = txn.cursor(env.open_db("children", txn))
            get_content = lambda node_id: loads(content_cur.get(node_id.encode()))[0]
            get_children = lambda node_id: loads(children_cur.get(node_id.encode()))[0]

        buffer_lines = []
        stack = [view + (0,)]
        while stack:
            node_id, sub_tree, level = stack.pop()
            expanded = bool(sub_tree)
            buffer_lines.extend(content_lines_to_buffer_lines(get_content(node_id), node_id, level, expanded))
            if expanded:
                children_ids = get_children(node_id)

                new_children_ids = children_ids - sub_tree.keys()
                for new_children_id in new_children_ids:
                    stack.append((new_children_id, {}, level + 1))

                sub_tree_children_ids = sub_tree.keys()
                for child_id in reversed(sub_tree_children_ids & children_ids):
                    stack.append((child_id, sub_tree[child_id], level + 1))

        return buffer_lines


def get_nodes(node_ids: list[NodeId]) -> list[tuple[Node, ContentRev, ChildrenRev]]:
    nodes = []
    with lmdb.open(DB_FILE) as env:
        with env.begin() as txn:
            content_cur = txn.cursor(env.open_db("content", create=False))
            children_cur = txn.cursor(env.open_db("children", create=False))
            for node_id in node_ids:
                content_rev: ContentRev
                content_lines, content_rev = loads(content_cur.get(node_id.encode()))
                children_rev: ChildrenRev
                children_ids, children_rev = loads(children_cur.get(node_id.encode()))
                nodes.append((Node(node_id, content_lines, children_ids), content_rev, children_rev))
    return nodes


def save_nodes_to_db(changed_nodes: dict[NodeId, Node], root_view: tuple[NodeId, dict[str, dict]]):
    with lmdb.open(DB_FILE) as env:
        with env.begin(write=True) as txn:
            content_cur = txn.cursor(env.open_db("content", txn))
            children_cur = txn.cursor(env.open_db("children", txn))
            for node_id, node in changed_nodes.items():
                content_conflict = put_data_to_cursor(content_cur, node_id, node.content_lines, Utils.conflict)
                if content_conflict:
                    cur_state.content_conflict_node_ids.add(node_id)
                children_conflict = put_data_to_cursor(children_cur, node_id, node.children_ids,
                                                       lambda n, o: n.union(o))
                if children_conflict:
                    cur_state.children_conflict_node_ids.add(node_id)

            views_cur = txn.cursor(env.open_db("views", txn))
            if views_cur.last():
                key_bytes = views_cur.key()
                new_key = int.from_bytes(key_bytes, 'big') + 1
            else:
                new_key = 0
            views_cur.put(new_key.to_bytes(new_key.bit_length(), 'big'), dumps(root_view))


def put_data_to_cursor(db_cursor: lmdb.Cursor, node_id: NodeId, new_data: Any, conflict_resolver: Callable) -> bool:
    did_conflict = False
    db_node_data_json = db_cursor.get(node_id)
    if db_node_data_json is None:
        new_data_rev = 0
    else:
        db_node_data, db_data_rev = loads(db_node_data_json)
        new_data_rev = db_data_rev + 1
        if db_data_rev != ledger.last_node_content_rev[node_id]:
            new_data = conflict_resolver(new_data, db_node_data)
            did_conflict = True
    db_cursor.put(node_id, dumps([new_data, new_data_rev]).encode())
    return did_conflict


@dataclass
class Ledger:
    # Created during each buffer writing event
    last_node_children_set: dict[NodeId, FrozenSet[NodeId]]
    last_node_content_rev: dict[NodeId, int]
    last_node_children_rev: dict[NodeId, int]
    buffer_node_id_map: dict[BufferNodeId, NodeId]
    node_buffer_id_map: dict[NodeId, BufferNodeId]
    last_node_content_hash: dict[NodeId, Union[bytes, None]]


ledger = Ledger({}, {}, {}, {}, {}, defaultdict(lambda: None))


class ProcessState:
    def __init__(self):
        self.changed_nodes: dict[NodeId, Node] = {}
        self.content_conflict_node_ids: set[str] = set()
        self.children_conflict_node_ids: set[str] = set()


cur_state = ProcessState()


class Utils:
    def __init__(self):
        pass

    @staticmethod
    def split_id_from_line(line: str) -> Tuple[Union[NodeId, None], str]:
        id_regex = compile("<!--(.+)--> {2}")
        id_match = id_regex.match(line)
        if id_match:
            buffer_node_id = cast(BufferNodeId, id_match.group(1))
            node_id = ledger.buffer_node_id_map[buffer_node_id]
            line = line.removeprefix(id_match.group(0))
        else:
            node_id = None
        return node_id, line

    # conflict = Differ().compare
    @staticmethod
    def conflict(new_lines: list[str], old_lines: list[str]) -> list[str]:
        return new_lines + ["\n<!-- CONFLICT -->\n"] + old_lines

    @staticmethod
    def process_node(node: Node):
        content_lines = Node.content_lines
        content_hash = sha256('\n'.join(content_lines).encode()).digest()
        node_id = node.node_id
        is_content_changed = ledger.last_node_content_hash[node_id] != content_hash

        new_children = node.children_ids - ledger.last_node_children_set[node_id]

        if not (is_content_changed or new_children):  # Assuming real-time update else check node_previously changed
            return

        node_previously_changed = node_id in cur_state.changed_nodes

        if node_previously_changed:
            if is_content_changed:
                cur_state.changed_nodes[node_id].content_lines = Utils.conflict(content_lines,
                                                                                cur_state.changed_nodes[
                                                                                    node_id].content_lines)
                cur_state.content_conflict_node_ids.add(node_id)
            if new_children:
                cur_state.changed_nodes[node_id].children_ids.update(new_children)
                cur_state.children_conflict_node_ids.add(node_id)
        else:
            cur_state.changed_nodes[node_id] = node


def process_text(lines: list[str]) -> View:
    md_ast = get_md_ast("\n".join(lines))
    assert md_ast.last_child is not None

    root_subtree = dict()
    if md_ast.last_child.t == "list":  # The children
        root_last_line_num = md_ast.last_child.sourcepos[0][0] - 1
        assert md_ast.first_child != md_ast.last_child  # root node has some content
        list_nodes: list[tuple[Any, dict]] = [(md_ast.last_child.first_child, root_subtree)]
        while list_nodes:
            list_node, list_children = list_nodes.pop()
            cur_list_item_node = list_node.last_child
            while cur_list_item_node:
                content_start_line_num = cur_list_item_node.sourcepos[0][0] - 1
                content_indent = cur_list_item_node.sourcepos[0][1] + 1
                node_id, id_line = Utils.split_id_from_line(lines[content_start_line_num][content_indent:])

                if node_id is None:
                    node_id = get_node_id()
                list_item_node_children_ids = list_children[node_id] = {}

                if cur_list_item_node.last_child.t == "list":
                    content_end_line_num = cur_list_item_node.last_child.source[0][0] - 1
                    list_nodes.append((cur_list_item_node.last_child, list_item_node_children_ids))
                else:
                    content_end_line_num = cur_list_item_node.sourcepos[1][0]

                content_lines = ([id_line] if id_line else []) + [
                    line.removeprefix(" " * content_indent)
                    for line in lines[content_start_line_num + 1: content_end_line_num]
                ]

                node = Node(node_id, content_lines, set(list_item_node_children_ids))
                Utils.process_node(node)

                cur_list_item_node = cur_list_item_node.prv
    else:
        root_last_line_num = None

    root_node_id, root_id_line = Utils.split_id_from_line(lines[0])
    assert root_node_id is not None
    root_content_lines = lines[:root_last_line_num]
    root_node = Node(root_node_id, root_content_lines, set(root_subtree))
    Utils.process_node(root_node)

    root_view: View = cast(View, (root_node_id, root_subtree))

    return root_view


root = process_text(
    """
<!--1-->  Flat text line
* Node 1
* <!--🧑-->  
    * laskdfj
* Node 2
""".strip(
        "\n"
    ).split(
        "\n"
    )
)

print(root)

exit()
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
"""
