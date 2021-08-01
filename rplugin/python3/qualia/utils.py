from base64 import urlsafe_b64encode, urlsafe_b64decode
from difflib import SequenceMatcher
from hashlib import sha256
from json import loads, dumps
from logging import getLogger
from logging.handlers import RotatingFileHandler
from os import symlink
from os.path import basename
from pathlib import Path
from re import compile, search, split
from secrets import token_urlsafe, token_bytes
from subprocess import run, CalledProcessError
from tempfile import gettempdir
from time import time_ns, sleep
from typing import Callable, Union, Iterable, cast, TextIO
from uuid import uuid4, UUID

import lmdb
from lmdb import Cursor
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode
from orderedset import OrderedSet
from pynvim import Nvim
from pynvim.api import Buffer

from qualia.config import DB_FOLDER, LEVEL_SPACES, EXPANDED_BULLET, COLLAPSED_BULLET, TO_EXPAND_BULLET, GIT_FOLDER, \
    ROOT_ID_KEY, APP_FOLDER_PATH, FILE_FOLDER, GIT_BRANCH, CLIENT_KEY, LOG_FILENAME, CONTENT_CHILDREN_SEPARATOR_LINES, \
    GIT_TOKEN_URL, GIT_SEARCH_URL
from qualia.models import Client, NodeData
from qualia.models import NodeId, JSONType, BufferNodeId, NODE_ID_ATTR, Tree, Cursors, UncertainNodeChildrenException, \
    LastSeen, DuplicateNodeException

_md_parser = MarkdownIt().parse

logger = getLogger("qualia")
file_handler = RotatingFileHandler(filename=LOG_FILENAME, mode='w', maxBytes=512000, backupCount=4)
logger.addHandler(file_handler)


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


def batch_undo(nvim: Nvim):
    first_change = True
    while True:
        if first_change:
            first_change = False
        else:
            nvim.command("undojoin")
        yield


class Database:
    """
    For some reason `Database` cannot be nested. E.g. if nesting in save_bloom_filter(), the db is empty on next run.
    Relevant?
    > Repeat Environment.open_db() calls for the same name will return the same handle.
    """

    def __enter__(self) -> Cursors:
        db_names = ("content", "children", "views", "unsynced_content", "unsynced_children", "unsynced_views",
                    "buffer_to_node_id", "node_to_buffer_id", "metadata", "bloom_filters")
        self.env = env = lmdb.open(DB_FOLDER.as_posix(), max_dbs=len(db_names))
        self.txn = env.begin(write=True)
        cursors = Cursors(**{db_name: self.sub_db(db_name) for db_name in db_names})
        return cursors

    def sub_db(self, db_name: str) -> lmdb.Cursor:
        return self.txn.cursor(self.env.open_db(db_name.encode(), self.txn))

    def __exit__(self, *args) -> None:
        self.txn.__exit__(*args)
        self.env.__exit__(*args)


def children_hash(children: set[NodeId]):
    return sha256('\n'.join(sorted(children)).encode()).digest()


def content_hash(content_lines: list[str]):
    return sha256('\n'.join(content_lines).encode()).digest()


def conflict(new_lines: list[str], old_lines: list[str], no_check: bool) -> list[str]:
    return ["<<<<<<< OLD"] + old_lines + ["======="] + new_lines + [
        ">>>>>>> NEW"] if no_check or new_lines != old_lines else new_lines


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
    id_regex = compile(r"\[]\(q://(.+?)\) {2}")
    id_match = id_regex.match(line)
    if id_match:
        line = line.removeprefix(id_match.group(0))
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
        if cur_type == last_type:
            last_child_list_ast = merged_child_asts[-1]
            last_child_list_ast.children.extend(cur_child_list_ast.children)

            token_obj = last_child_list_ast.token or last_child_list_ast.nester_tokens.opening
            token_obj.map = last_child_list_ast.map[0], cur_child_list_ast.map[1]

            for child_ast in cur_child_list_ast.children:
                child_ast.parent = last_child_list_ast
        else:
            merged_child_asts.append(cur_child_list_ast)
        last_type = cur_type
    return merged_child_asts


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
    logger.debug(f"GIT:\n{stdout}\n")
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
        except OSError:
            return False
        src.unlink(src)
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


def name_to_node_id(name: str, remove_suffix: str) -> NodeId:
    if name.endswith(remove_suffix):
        node_id_hex = name.removesuffix(remove_suffix)
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
        put_key_val(ROOT_ID_KEY, root_id, cursors.metadata, False)


def setup_repository(metadata_cursor: Cursor) -> None:
    try:
        run_git_cmd(["rev-parse", "--is-inside-work-tree"])
    except CalledProcessError:
        run_git_cmd(["init"])
        run_git_cmd(["checkout", "-b", GIT_BRANCH])
        try:
            run_git_cmd(["pull", GIT_TOKEN_URL, GIT_BRANCH])
        except CalledProcessError:
            pass
        gitattributes_path = GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            with open(gitattributes_path, 'x') as f:
                f.write("*.md merge=union\n* text=auto eol=lf\n")
            run_git_cmd(["add", "-A"])
            run_git_cmd(["commit", "-m", "bootstrap"])
        client_data = Client(**get_key_val(CLIENT_KEY, metadata_cursor))
        run_git_cmd(["config", "user.name", client_data["client_name"]])
        run_git_cmd(["config", "user.email", f"{client_data['client_id']}@q.client"])


symlinks_enabled = _check_symlinks_enabled()


def set_client_if_new(metadata_cursor: Cursor):
    if metadata_cursor.get(CLIENT_KEY.encode()) is None:
        client_details = Client(client_id=str(get_uuid()), client_name=f"Vim-{token_urlsafe(1)}")
        put_key_val(CLIENT_KEY, client_details, metadata_cursor, False)


def node_id_to_hex(node_id) -> str:
    return str(UUID(urlsafe_b64decode(node_id).hex()))


def create_directory_if_absent(directory_path: Path):
    try:
        directory_path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (directory_path.is_symlink() and directory_path.is_dir()):
            raise Exception(f"{directory_path} already exists as a file.")


def resolve_main_id(buffer: Buffer, cursors: Cursors) -> NodeId:
    file_name = basename(buffer.name)
    try:
        main_id = name_to_node_id(file_name, '.q.md')
        if get_key_val(main_id, cursors.content) is None:
            raise ValueError
    except ValueError:
        root_id = cast(NodeId, get_key_val(ROOT_ID_KEY, cursors.metadata))
        assert root_id
        buffer.name = node_id_to_filename(root_id)
        main_id = root_id
    return main_id


def node_id_to_filename(root_id: NodeId) -> str:
    return FILE_FOLDER.joinpath(node_id_to_hex(root_id) + ".q.md").as_posix()


def bootstrap() -> None:
    create_directory_if_absent(APP_FOLDER_PATH)
    create_directory_if_absent(FILE_FOLDER)
    with Database() as cursors:
        set_client_if_new(cursors.metadata)
        setup_repository(cursors.metadata)
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


def render_buffer(buffer: Buffer, new_content_lines: list[str], nvim: Nvim) -> list[str]:
    old_content_lines = list(buffer)
    undojoin = batch_undo(nvim)
    offset = 0
    for opcode, old_i1, old_i2, new_i1, new_i2 in SequenceMatcher(a=old_content_lines, b=new_content_lines,
                                                                  autojunk=False).get_opcodes():
        if opcode == "replace":
            num_old_lines = old_i2 - old_i1
            num_new_lines = new_i2 - new_i1
            min_lines = min(num_old_lines, num_new_lines)
            # setline preserves the marks unlike buffer[lnum] = "content"
            next(undojoin)
            assert nvim.funcs.setline(min(old_i1 + offset, len(buffer)) + 1,
                                      new_content_lines[new_i1:new_i1 + min_lines]) == 0
            if num_new_lines > num_old_lines:
                next(undojoin)
                idx = old_i1 + min_lines + offset
                buffer[idx:idx] = new_content_lines[new_i1 + min_lines: new_i2]
            elif num_new_lines < num_old_lines:
                next(undojoin)
                del buffer[old_i1 + min_lines + offset:old_i2 + offset]
            offset += num_new_lines - num_old_lines
        elif opcode == "insert":
            next(undojoin)
            buffer[old_i1 + offset:old_i1 + offset] = new_content_lines[new_i1:new_i2]
            offset += new_i2 - new_i1
        elif opcode == "delete":
            next(undojoin)
            del buffer[old_i1 + offset:old_i2 + offset]
            offset -= old_i2 - old_i1
    return old_content_lines


def normalized_prefixes(string: str):
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}
