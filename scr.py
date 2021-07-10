from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple, Union
from time import time_ns
from secrets import token_urlsafe
# from difflib import Differ
from hashlib import sha256
from re import compile

from commonmark import Parser


def get_uuid():
    return urlsafe_b64encode(uuid4().bytes).rstrip(b"=").decode("ascii")


def get_time_uuid():
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    return urlsafe_b64encode(left_padded_time).decode() + token_urlsafe(10)

get_node_id = get_time_uuid
get_md_ast = Parser().parse

@dataclass
class State:
    new_nodes: dict[str,list[str]] = {}
    id_map: dict[str,str] = {}
    stored_hash: dict[str,bytes] = defaultdict(lambda: sha256(b''))
    changes: dict[str,list[str]] = {}

state = State()

class Utils:
    @staticmethod
    def get_line_id(line: str) -> Tuple[Union[str, None], str]:
        id_regex = compile("<!--(.+)--> {2}")
        id_match = id_regex.match(line)
        if id_match:
            node_id = state.id_map[id_match.group(1)]
            line = line.removeprefix(id_match.group(0))
        else:
            node_id = None
        return node_id, line

    @staticmethod
    def process_node(node_id: Union[str, None], content: list[str]):
        #conflict = Differ().compare
        def conflict(new: list[str], node_id: str):
            CONFLICTS = "conflicts"
            old = state.changes[node_id]
            state.changes[CONFLICTS] += node_id
            return new + ["\n<!-- CONFLICT -->\n"] + old

        content_hash = sha256('\n'.join(content).encode()).digest()
        if node_id:
            if node_id in state.changes and state.stored_hash[node_id] != content_hash:
                state.changes[node_id] = conflict(content, node_id)
            else:
                state.changes[node_id] = content
        else:
            state.new_nodes[get_node_id()] = content

def process_text(lines:list[str]):
    get_line_id = Utils.get_line_id
    process_node = Utils.process_node

    def process_list_nodes(lines:list[str], list_nodes):
        while list_nodes:
            list_node, list_children = list_nodes.pop()
            list_item_node = list_node.last_child
            while list_item_node:
                content_start_line_num = list_item_node.sourcepos[0][0] - 1
                content_indent = list_item_node.sourcepos[0][1] + 1
                node_id, id_line = get_line_id(lines[content_start_line_num][content_indent:])

                list_item_node_children = list_children[node_id] = {}

                if list_item_node.last_child.t == "list":
                    content_end_line_num = list_item_node.last_child.source[0][0] - 1
                    list_nodes.append((list_item_node.last_child, list_item_node_children))
                else:
                    content_end_line_num = list_item_node.sourcepos[1][0]

                content_lines = ([id_line] if id_line else []) + [
                    line.removeprefix(" " * content_indent)
                    for line in lines[content_start_line_num + 1: content_end_line_num]
                ]

                process_node(node_id, content_lines)

                list_item_node = list_item_node.prv

    md_ast = get_md_ast("\n".join(lines))
    assert md_ast.last_child is not None

    root_children = {}
    if md_ast.last_child.t == "list":  # The children
        root_last_line_num = md_ast.last_child.sourcepos[0][0] - 1
        assert md_ast.first_child != md_ast.last_child  # root node has some content
        list_nodes = [(md_ast.last_child.first_child, root_children)]
        process_list_nodes(lines, list_nodes)
    else:
        root_last_line_num = None

    root_node_id, root_id_line = get_line_id(lines[0])
    root_content_lines = lines[0 if root_id_line else 1:root_last_line_num]
    if root_node_id is None:
        raise
    process_node(root_node_id, root_content_lines)

    return root_node_id, root_children




root = text_to_node(
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


nvim = attach("socket", path=r"\\.\pipe\nvim-15176-0")  # "/tmp/nvim")
call = nvim.call

exit()

current_cursor = nvim.current.window.cursor

call("setloclist", 0, [{"lnum": 2, "col": 1, "text": "haha"}])
loclist = call("getloclist")

mark_ns = call("nvim_create_namespace", "qualia")
mark_id = call("nvim_buf_set_extmark", 0, mark_ns, 0, 0, {})

call("nvim_buf_get_extmark_by_id", 0, mark_ns, mark_id, {})
call("nvim_buf_get_extmarks", 0, mark_ns, 0, -1, {})

from ptpython import embed

embed(globals(), locals())
