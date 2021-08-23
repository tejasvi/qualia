from __future__ import annotations

from _sha256 import sha256
from base64 import urlsafe_b64encode, urlsafe_b64decode
from difflib import SequenceMatcher
from hashlib import sha256
from json import loads, dumps
from logging import getLogger
from os import symlink
from os.path import basename
from pathlib import Path
from re import compile, search, split
from secrets import token_urlsafe, token_bytes
from subprocess import run, CalledProcessError
from tempfile import gettempdir
from threading import Thread, Event
from time import time_ns, sleep, time
from typing import Callable, Union, Iterable, cast, TextIO, Optional, Iterator
from uuid import uuid4, UUID

import lmdb
from lmdb import Cursor, Environment
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode
from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import DB_FOLDER, LEVEL_SPACES, EXPANDED_BULLET, COLLAPSED_BULLET, TO_EXPAND_BULLET, GIT_FOLDER, \
    ROOT_ID_KEY, APP_FOLDER_PATH, FILE_FOLDER, GIT_BRANCH, CLIENT_KEY, CONTENT_CHILDREN_SEPARATOR_LINES, \
    GIT_TOKEN_URL, GIT_SEARCH_URL, GIT_URL, DEBUG
from qualia.models import Client, NodeData, RealtimeData
from qualia.models import NodeId, JSONType, BufferNodeId, NODE_ID_ATTR, Tree, Cursors, UncertainNodeChildrenException, \
    LastSeen, DuplicateNodeException

_md_parser = MarkdownIt("zero", {"maxNesting": float('inf')}).enable(
    ["link", "list", "code", "fence", "html_block"]).parse

logger = getLogger("qualia")


# logger.setLevel(logging.DEBUG)


def get_md_ast(content_lines: list[str]) -> SyntaxTreeNode:
    root_ast = SyntaxTreeNode(_md_parser('\n'.join(content_lines)))
    root_ast.token = Token(meta={}, map=[0, len(content_lines)], nesting=0, tag="", type="root")
    return root_ast


def get_uuid() -> NodeId:
    return cast(NodeId, urlsafe_b64encode(uuid4().bytes).decode())


def get_time_uuid() -> NodeId:
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    id_bytes = left_padded_time + token_bytes(10)
    return cast(NodeId, urlsafe_b64encode(id_bytes).decode())


get_random_id: Callable[[], NodeId] = get_time_uuid


def get_node_id() -> NodeId:
    while True:
        node_id = get_random_id()
        if ")" not in node_id:
            break
    return node_id


def batch_undo(nvim: Nvim) -> Iterator[None]:
    first_change = True
    while True:
        if first_change:
            first_change = False
        else:
            nvim.command("undojoin")
        yield


class Database:
    """
    For some reason environment cannot be nested therefore . E.g. if nesting in save_bloom_filter(), the db is empty on next run.
    Relevant?
    > Repeat Environment.open_db() calls for the same name will return the same handle.
    """
    _db_names = (
        "content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views", "buffer_to_node_id",
        "node_to_buffer_id", "metadata", "bloom_filters", "parents", "inverted_views")
    _env: Optional[Environment] = None

    def __init__(self) -> None:
        # Environment not initialized in class definition to prevent race with folder creation
        if Database._env is None:
            Database._env = lmdb.open(DB_FOLDER.as_posix(), max_dbs=len(Database._db_names), map_size=2 ** 20)

    def __enter__(self) -> Cursors:
        self.txn = self._env.begin(write=True)
        cursors = Cursors(**{db_name: self.sub_db(db_name) for db_name in Database._db_names})
        return cursors

    def sub_db(self, db_name: str) -> lmdb.Cursor:
        return self.txn.cursor(Database._env.open_db(db_name.encode(), self.txn))

    def __exit__(self, *args) -> None:
        self.txn.__exit__(*args)


def children_hash(children: set[NodeId]):
    return sha256('\n'.join(sorted(children)).encode()).digest()


def content_hash(content_lines: list[str]):
    return sha256('\n'.join(content_lines).encode()).digest()


def conflict(new_lines: list[str], old_lines: list[str]) -> list[str]:
    return ["<<<<<<< OLD"] + old_lines + ["======="] + new_lines + [
        ">>>>>>> NEW"] if new_lines != old_lines else new_lines


def get_key_val(key: Union[str, bytes], cursor: lmdb.Cursor) -> JSONType:
    value_bytes = cursor.get(key if isinstance(key, bytes) else key.encode())
    return None if value_bytes is None else loads(value_bytes.decode())


def put_key_val(key: Union[str, bytes], val: JSONType, cursor: lmdb.Cursor, overwrite) -> None:
    cursor.put(key if isinstance(key, bytes) else key.encode(), dumps(val).encode(), overwrite=overwrite)


def node_to_buffer_id(node_id: NodeId) -> BufferNodeId:
    return BufferNodeId(node_id)
    # buffer_node_id = get_key_val(node_id, cursors.buffer_to_node_id)
    # if buffer_node_id is None:
    #     if cursors.buffer_to_node_id.last():
    #         last_buffer_id_bytes = cursors.buffer_to_node_id.key()
    #         new_counter = int.from_bytes(last_buffer_id_bytes, 'big') + 1
    #         buffer_id_bytes = new_counter.to_bytes(32, 'big').decode()
    #     else:
    #         buffer_id_bytes = (0).to_bytes(32, 'big')
    #     buffer_node_id = base65536.encode(buffer_id_bytes)
    #     # base65536 doesn't output brackets https://qntm.org/safe
    #     put_key_val(node_id, buffer_node_id, cursors.node_to_buffer_id)
    # return buffer_node_id


def buffer_to_node_id(buffer_id: BufferNodeId) -> Union[None, NodeId]:
    return NodeId(buffer_id)
    # buffer_id_bytes = base65536.decode(buffer_id)
    # return state.cursors.buffer_to_node_id.get(buffer_id_bytes)


def get_id_line(line: str) -> tuple[NodeId, str]:
    id_regex = compile(r"\[]\(q://(.+?)\) {0,2}")
    id_match = id_regex.match(line)
    if id_match:
        line = removeprefix(line, id_match.group(0))
        buffer_node_id = BufferNodeId(id_match.group(1))
        node_id = buffer_to_node_id(buffer_node_id)
    else:
        node_id = get_node_id()
    return node_id, line


def content_lines_to_buffer_lines(content_lines: list[str], node_id: NodeId, level: int, expanded: bool,
                                  ordered: bool) -> list[str]:
    buffer_id = node_to_buffer_id(node_id)
    if level == 0:
        buffer_lines = content_lines
    else:
        offset = 3 if ordered else 2
        space_count = LEVEL_SPACES * (level - 1) + offset
        space_prefix = ' ' * space_count
        buffer_lines = [space_prefix[:-offset]
                        + f"{'1.' if ordered else (EXPANDED_BULLET if expanded else COLLAPSED_BULLET)} [](q://{buffer_id})  "
                        + content_lines[0]]
        for idx, line in enumerate(content_lines[1:]):
            buffer_lines.append(space_prefix + line)
    return buffer_lines


def previous_sibling_node_line_range(list_item_ast: SyntaxTreeNode, node_id: NodeId) -> tuple[int, int]:
    while True:
        assert list_item_ast.previous_sibling, (node_id, list_item_ast.map)
        if list_item_ast.previous_sibling.meta[NODE_ID_ATTR] == node_id:
            node_loc = list_item_ast.previous_sibling.map
            break
        list_item_ast = list_item_ast.previous_sibling
    return node_loc


def raise_if_duplicate_sibling(list_item_ast: SyntaxTreeNode, node_id: NodeId, tree: Tree) -> None:
    if node_id in tree:
        sibling_line_range = previous_sibling_node_line_range(list_item_ast, node_id)
        raise DuplicateNodeException(node_id, (list_item_ast.map, sibling_line_range))


def get_ast_sub_lists(list_item_ast: SyntaxTreeNode) -> list[
    SyntaxTreeNode]:  # TODO: Merge two loops, line range updation here instead of process list asts?
    # FIX: Can't call twice -> reduce to one node
    child_list_asts = []
    if list_item_ast.children:
        cur_child_list_ast = list_item_ast.children[-1]
        while cur_child_list_ast.type.endswith("_list"):
            child_list_asts.append(cur_child_list_ast)
            cur_child_list_ast = cur_child_list_ast.previous_sibling
            if not cur_child_list_ast or cur_child_list_ast is list_item_ast.children[0]:
                break
    child_list_asts.reverse()

    merged_child_asts = merge_sibling_lists_ignore_bullets(child_list_asts)

    return merged_child_asts


def merge_sibling_lists_ignore_bullets(child_list_asts: list[SyntaxTreeNode]) -> list[SyntaxTreeNode]:
    last_type = None
    merged_child_asts: list[SyntaxTreeNode] = []
    for cur_child_list_ast in child_list_asts:
        cur_type = cur_child_list_ast.type
        if cur_type == last_type or (cur_type.endswith("_list") and last_type and last_type.endswith("_list")):
            last_child_list_ast = merged_child_asts[-1]
            last_nester_tokens = last_child_list_ast.nester_tokens

            token_obj = last_child_list_ast.token or last_nester_tokens.opening
            token_obj.map = last_child_list_ast.map[0], cur_child_list_ast.map[1]

            if cur_type == 'ordered_list' and last_type == "bullet_list":
                copy_list_ast_type(last_child_list_ast, cur_child_list_ast)

            last_child_list_ast.children.extend(cur_child_list_ast.children)

            for child_ast in cur_child_list_ast.children:
                child_ast.parent = last_child_list_ast

        else:
            merged_child_asts.append(cur_child_list_ast)
        last_type = cur_type
    return merged_child_asts


def copy_list_ast_type(target_list_ast: SyntaxTreeNode, source_list_ast: SyntaxTreeNode) -> None:
    # skip since markup used for finding indent
    # list_markup = source_list_ast.children[0].nester_tokens.closing.markup
    # for child_ast in target_list_ast.children:
    #     child_ast.nester_tokens.closing.markup = child_ast.nester_tokens.opening.markup = list_markup

    target_list_ast.nester_tokens.opening.type = source_list_ast.nester_tokens.opening.type
    target_list_ast.nester_tokens.closing.type = source_list_ast.nester_tokens.closing.type

    target_list_ast.nester_tokens.opening.tag = source_list_ast.nester_tokens.opening.tag
    target_list_ast.nester_tokens.closing.tag = source_list_ast.nester_tokens.closing.tag


def preserve_expand_consider_sub_tree(list_item_ast: SyntaxTreeNode, node_id: NodeId, sub_list_tree: Tree,
                                      last_seen: LastSeen):
    bullet = list_item_ast.markup

    parent_ast = list_item_ast.previous_sibling if (list_item_ast.parent.type == 'ordered_list'
                                                    and list_item_ast.previous_sibling) else list_item_ast.parent.parent
    parent_node_id = parent_ast.meta[NODE_ID_ATTR]

    not_new = parent_node_id in last_seen and node_id in last_seen[parent_node_id].children_ids

    if not_new:
        consider_sub_tree = bullet not in (COLLAPSED_BULLET, TO_EXPAND_BULLET)
    else:
        if sub_list_tree:
            if node_id in last_seen and sub_list_tree.keys() ^ last_seen[node_id].children_ids:
                raise UncertainNodeChildrenException(node_id, list_item_ast.map)
            else:
                consider_sub_tree = True
        else:
            consider_sub_tree = False

    expand = bullet == TO_EXPAND_BULLET or (bullet != COLLAPSED_BULLET and sub_list_tree)

    return expand, consider_sub_tree


def run_git_cmd(arguments: list[str]) -> str:
    result = run(["git"] + arguments, check=True, cwd=GIT_FOLDER, capture_output=True)
    stdout = '\n'.join([stream.decode() for stream in (result.stdout, result.stderr)]).strip()
    logger.critical(f"GIT:\n{stdout}\n")
    return stdout


def _check_symlinks_enabled() -> bool:
    temp_dir = Path(gettempdir())
    for try_num in range(100):
        src = temp_dir.joinpath(f'{try_num}.test.q')
        try:
            open(src, 'x').close()
            symlink_dest = temp_dir.joinpath('.symlink.test.q')
            symlink(src, symlink_dest)
            symlink_dest.unlink()
        except FileExistsError:
            continue
        except (NotImplementedError, OSError):
            return False
        src.unlink()
        return True


class LockNotAcquired(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class GitInit:
    def __enter__(self) -> None:
        max_tries = 3
        for tries in range(1, max_tries + 1):
            self.lock_file_path = Path(GIT_FOLDER).joinpath(".git/.qualia_lock")
            try:
                self.lock_file = open(self.lock_file_path, 'x')
            except FileExistsError:
                if tries == max_tries:
                    raise LockNotAcquired(
                        "Could not acquire lock probably due to previous program crash. "
                        f"Verify the data and then delete the Lock File: '{self.lock_file_path}' manually.")
                sleep(10)
            else:
                existing_branch = run_git_cmd(["branch", "--show-current"])
                if existing_branch == GIT_BRANCH:
                    self.existing_branch = None
                else:
                    self.existing_branch = existing_branch
                    run_git_cmd(["stash"])
                    run_git_cmd(["switch", "-c", GIT_BRANCH])
                break

    def __exit__(self, *_args) -> None:
        if self.existing_branch:
            run_git_cmd(["checkout", self.existing_branch])
            run_git_cmd(["stash", "pop"])
        self.lock_file.close()
        self.lock_file_path.unlink()


def is_valid_uuid(string: str) -> bool:
    try:
        UUID(string)
    except ValueError:
        return False
    return True


# pre 3.9 str.removeprefix
def removesuffix(input_string: str, suffix: str) -> str:
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


def removeprefix(input_string: str, suffix: str) -> str:
    if suffix and input_string.startswith(suffix):
        return input_string[len(suffix):]
    return input_string


def name_to_node_id(name: str, remove_suffix: str) -> NodeId:
    if name.endswith(remove_suffix):
        node_id_hex = removesuffix(name, remove_suffix)
    else:
        raise ValueError
    node_id = NodeId(urlsafe_b64encode(UUID(node_id_hex).bytes).decode())
    return node_id


def add_content_to_node_directory(content_lines: list[str], node_directory_path: Path):
    with open(node_directory_path.joinpath("README.md"), 'x') as content_file:
        content_file.write('\n'.join(content_lines) + '\n')


def add_children_to_node_directory(node_children_ids: Iterable[NodeId], node_directory_path: Path):
    for child_node_id in node_children_ids:
        hex_id = node_id_to_hex(child_node_id)
        child_path = node_directory_path.joinpath(hex_id + ".q")
        symlink_source = f"../{hex_id}.q"
        if symlinks_enabled:
            symlink(symlink_source, child_path, target_is_directory=True)
        else:
            with open(child_path, 'x') as child_file:
                child_file.writelines([symlink_source])


def ensure_root_node(cursors: Cursors) -> None:
    if get_key_val(ROOT_ID_KEY, cursors.metadata) is None:
        root_id = get_time_uuid()
        put_key_val(root_id, [''], cursors.content, False)
        put_key_val(root_id, [], cursors.children, False)
        put_key_val(root_id, [], cursors.parents, False)
        put_key_val(ROOT_ID_KEY, root_id, cursors.metadata, False)


repository_exists = Event()


# @line_profiler_pycharm.profile
def setup_repository(client_data: Client) -> None:
    try:
        run_git_cmd(["rev-parse", "--is-inside-work-tree"])
    except CalledProcessError:
        run_git_cmd(["init"])
        run_git_cmd(["checkout", "-b", GIT_BRANCH])
        try:
            print("Pulling repository")
            start_time = time()
            run_git_cmd(["pull", GIT_TOKEN_URL, GIT_BRANCH])
            print(f"Pull took {time() - start_time} seconds")
        except CalledProcessError as e:
            print(f"Can't fetch from {GIT_URL}:{GIT_BRANCH}\nError: {repr(e)}")
        gitattributes_path = GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            with open(gitattributes_path, 'x') as f:
                f.write("*.md merge=union\n* text=auto eol=lf\n")
            run_git_cmd(["add", "-A"])
            run_git_cmd(["commit", "-m", "bootstrap"])
        run_git_cmd(["config", "user.name", client_data["client_name"]])
        run_git_cmd(["config", "user.email", f"{client_data['client_id']}@q.client"])
    repository_exists.set()


symlinks_enabled = _check_symlinks_enabled()


def set_client_if_new(metadata_cursor: Cursor):
    if metadata_cursor.get(CLIENT_KEY.encode()) is None:
        client_details = Client(client_id=str(get_uuid()), client_name=f"Vim-{token_urlsafe(1)}")
        put_key_val(CLIENT_KEY, client_details, metadata_cursor, False)


def node_id_to_hex(node_id: NodeId) -> str:
    return str(UUID(bytes=urlsafe_b64decode(node_id)))


def create_directory_if_absent(directory_path: Path):
    try:
        directory_path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (directory_path.is_symlink() and directory_path.is_dir()):
            raise Exception(f"{directory_path} already exists as a file.")


def buffer_inverted(buffer_name: str) -> bool:
    return basename(buffer_name)[0] == "~"


def node_id_to_filepath(root_id: NodeId, inverted) -> str:
    file_name = node_id_to_hex(root_id) + ".q.md"
    if inverted:
        file_name = '~' + file_name
    return FILE_FOLDER.joinpath(file_name).as_posix()


def bootstrap() -> None:
    for path in (APP_FOLDER_PATH, FILE_FOLDER, GIT_FOLDER, DB_FOLDER):
        create_directory_if_absent(path)
    # file_handler = RotatingFileHandler(filename=LOG_FILENAME, mode='w', maxBytes=512000, backupCount=4)
    # logger.addHandler(file_handler)
    logger.critical("STARTING")
    with Database() as cursors:
        set_client_if_new(cursors.metadata)
        # Get client data early since cursors are invalid in thread
        client_data = Client(**get_key_val(CLIENT_KEY, cursors.metadata))
        Thread(target=lambda: setup_repository(client_data)).start()
        ensure_root_node(cursors)


def file_children_line_to_node_id(line: str) -> NodeId:
    uuid_match = search(r"[0-9a-f]{8}(?:-?[0-9a-f]{4}){4}[0-9a-f]{8}(?=\.md\)$)", line)
    assert uuid_match, f"Child node ID for '{line}' couldn't be parsed"
    return name_to_node_id(uuid_match.group(), '')


def get_file_content_children(file: TextIO) -> tuple[list[str], OrderedSet]:
    lines = file.read().splitlines()
    children_ids = []
    while lines:
        line = lines.pop()
        if line == CONTENT_CHILDREN_SEPARATOR_LINES[1]:
            assert lines.pop() == CONTENT_CHILDREN_SEPARATOR_LINES[0]
            break
        children_ids.append(file_children_line_to_node_id(line))
    return lines, OrderedSet(reversed(children_ids))


def pop_unsynced_nodes(cursors: Cursors):
    last_seen = LastSeen()
    unsynced_children = cursors.unsynced_children
    if unsynced_children.first():
        while True:
            node_id: NodeId = unsynced_children.key().decode()
            unsynced_children.delete()
            children_ids = frozenset(get_key_val(node_id, cursors.children))
            if node_id in last_seen:
                last_seen[node_id].children_ids = children_ids
            else:
                last_seen[node_id] = NodeData([''], children_ids)
            if not unsynced_children.next():
                break
    unsynced_content = cursors.unsynced_content
    if unsynced_content.first():
        while True:
            node_id: NodeId = unsynced_content.key().decode()
            unsynced_content.delete()
            content_lines = get_key_val(node_id, cursors.content)
            if node_id in last_seen:
                last_seen[node_id].content_lines = content_lines
            else:
                last_seen[node_id] = NodeData(content_lines, frozenset())
            if not unsynced_content.next():
                break
    return last_seen


def create_markdown_file(cursors: Cursors, node_id: NodeId) -> list[NodeId]:
    content_lines: list[str] = get_key_val(node_id, cursors.content)
    content_lines.extend(CONTENT_CHILDREN_SEPARATOR_LINES)
    node_children_ids: list[NodeId] = get_key_val(node_id, cursors.children) or []
    content_lines.append(f"0. [`Backlinks`]({GIT_SEARCH_URL + node_id_to_hex(node_id)})")
    for i, child_id in enumerate(node_children_ids):
        hex_id = node_id_to_hex(child_id)
        content_lines.append(f"{i}. [`{hex_id}`]({hex_id}.md)")
    with open(GIT_FOLDER.joinpath(node_id_to_hex(node_id) + ".md"), 'w') as f:
        f.write('\n'.join(content_lines) + '\n')
    return node_children_ids


def get_replace_buffer_line(nvim: Nvim) -> Callable[[int, Union[str, list[str]]], None]:
    setline = nvim.funcs.setline

    def replace_buffer_line(zero_indexed: int, content: Union[str, list[str]]) -> None:
        assert setline(zero_indexed + 1, content) == 0

    return replace_buffer_line


def render_buffer(buffer: Buffer, new_content_lines: list[str], nvim: Nvim) -> list[str]:
    old_content_lines = list(buffer)
    # Pre-Check common state with == (100x faster than loop)
    if old_content_lines != new_content_lines:
        line_num = 0
        for line_num, (old_line, new_line) in enumerate(zip(old_content_lines, new_content_lines)):
            if old_line != new_line:
                break

        undojoin = batch_undo(nvim)
        next(undojoin)

        set_buffer_line = get_replace_buffer_line(nvim)

        line_new_end, line_old_end = different_item_from_end(new_content_lines, old_content_lines, line_num)

        if line_num in (line_old_end, line_new_end):
            if line_num == line_old_end:
                set_buffer_line(line_num, new_content_lines[line_num])
                buffer[line_num + 1:line_num + 1] = new_content_lines[line_num + 1:line_new_end + 1]
            else:
                set_buffer_line(line_num, new_content_lines[line_num])
                del buffer[line_num + 1:line_old_end + 1]
        else:
            if (len(old_content_lines) - line_num) * (len(new_content_lines) - line_num) > 1e5:
                buffer[line_num:] = new_content_lines[line_num:]
            else:
                print("Surgical")
                surgical_render(buffer, new_content_lines, set_buffer_line, old_content_lines, undojoin)
    if DEBUG:
        try:
            assert new_content_lines == list(buffer)
        except AssertionError:
            buffer[:] = old_content_lines
            render_buffer(buffer, new_content_lines, nvim)
    return old_content_lines


def surgical_render(buffer: Buffer, new_content_lines: list[str],
                    replace_buffer_line: Callable[[int, Union[str, list[str]]], None],
                    old_content_lines: list[str], undojoin: Iterator) -> None:
    offset = 0
    for opcode, old_i1, old_i2, new_i1, new_i2 in SequenceMatcher(a=old_content_lines, b=new_content_lines,
                                                                  autojunk=False).get_opcodes():
        if opcode == "equal":
            continue
        next(undojoin)
        if opcode == "replace":
            num_old_lines = old_i2 - old_i1
            num_new_lines = new_i2 - new_i1
            min_lines = min(num_old_lines, num_new_lines)
            # setline preserves the marks unlike buffer[lnum] = "content"
            replace_buffer_line(min(old_i1 + offset, len(buffer)), new_content_lines[new_i1:new_i1 + min_lines])
            if num_new_lines != num_old_lines:
                next(undojoin)
                if num_new_lines > num_old_lines:
                    idx = old_i1 + min_lines + offset
                    buffer[idx:idx] = new_content_lines[new_i1 + min_lines: new_i2]
                elif num_new_lines < num_old_lines:
                    del buffer[old_i1 + min_lines + offset:old_i2 + offset]
            offset += num_new_lines - num_old_lines
        elif opcode == "insert":
            buffer[old_i1 + offset:old_i1 + offset] = new_content_lines[new_i1:new_i2]
            offset += new_i2 - new_i1
        elif opcode == "delete":
            del buffer[old_i1 + offset:old_i2 + offset]
            offset -= old_i2 - old_i1


def different_item_from_end(list1: list, list2: list, minimum_idx: int) -> tuple[int, int]:
    len1 = len(list1)
    len2 = len(list2)
    maximum_idx_rev = min(len1 - minimum_idx, len2 - minimum_idx) - 1

    for i_rev, (item1, item2) in enumerate(zip(reversed(list1), reversed(list2))):
        if item1 != item2 or i_rev == maximum_idx_rev:
            break
    else:
        raise ValueError("Lists are same")

    idx1 = len(list1) - i_rev - 1
    idx2 = len(list2) - i_rev - 1

    return idx1, idx2


def normalized_prefixes(string: str):
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}


def merge_children_with_local(node_id: NodeId, new_children_ids: Iterable[NodeId], children_cur: Cursor) -> list[
    NodeId]:
    merged_children_ids = OrderedSet(get_key_val(node_id, children_cur))
    merged_children_ids.update(new_children_ids)
    return list(merged_children_ids)


def merge_content_with_local(node_id: NodeId, new_content_lines: list[str], content_cur: Cursor) -> list[str]:
    db_content_lines: list[str] = get_key_val(node_id, content_cur)
    return conflict(new_content_lines, db_content_lines)


def value_hash(key: str, cursor: Cursor) -> str:
    data_bytes = cursor.get(key.encode())
    return realtime_data_hash(data_bytes)


def realtime_data_hash(data: Union[bytes, JSONType]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def sync_with_realtime_db(data: RealtimeData, realtime_session) -> None:
    if data and realtime_session.others_online:
        def broadcast_closure() -> None:
            realtime_session.client_broadcast(data)

        Thread(target=broadcast_closure).start()


def remove_absent_keys(dictionary: Tree, keys: OrderedSet[NodeId]):
    for key in dictionary.keys() - keys:
        dictionary.pop(key)


def resolve_main_id(buffer_name: str, content_cursor: Cursor) -> tuple[NodeId, bool]:
    file_name = basename(buffer_name)
    inverted = buffer_inverted(buffer_name)
    if inverted:
        file_name = file_name[1:]
    main_id = name_to_node_id(file_name, '.q.md')
    if get_key_val(main_id, content_cursor) is None:
        raise ValueError(buffer_name)
    return main_id, inverted
