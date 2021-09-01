from functools import cache
from os import symlink
from pathlib import Path
from re import search
from signal import getsignal, SIGTERM, SIG_DFL, signal, Signals
from tempfile import gettempdir
from time import sleep
from types import FrameType
from typing import Iterable, TextIO, cast

from orderedset import OrderedSet

from qualia.config import GIT_SEARCH_URL, _GIT_FOLDER, GIT_BRANCH
from qualia.models import NodeData, NodeId, Cursors, LastSync
from qualia.utils.common_utils import cd_run_git_cmd, node_id_to_hex, file_name_to_node_id, get_node_descendants, \
    get_node_content, logger, open_write_lf

_BACKLINK_LINE_START = "0. [`Backlinks`]"
_CONTENT_CHILDREN_SEPARATOR_LINES = ["<hr>", ""]


def add_content_to_node_directory(content_lines: list[str], node_directory_path: Path):
    with open_write_lf(node_directory_path.joinpath("README.md"), True) as content_file:
        content_file.write('\n'.join(content_lines) + '\n')


def add_children_to_node_directory(node_children_ids: Iterable[NodeId], node_directory_path: Path):
    for child_node_id in node_children_ids:
        hex_id = node_id_to_hex(child_node_id)
        child_path = node_directory_path.joinpath(hex_id + ".q")
        symlink_source = f"../{hex_id}.q"
        if symlinks_enabled():
            symlink(symlink_source, child_path, target_is_directory=True)
        else:
            with open_write_lf(child_path, True) as child_file:
                child_file.writelines([symlink_source])


@cache
def symlinks_enabled() -> bool:
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
        break
    return True


def create_markdown_file(cursors: Cursors, node_id: NodeId) -> OrderedSet[NodeId]:
    content_lines = cast(list[str], get_node_content(cursors, node_id))
    content_lines.extend(_CONTENT_CHILDREN_SEPARATOR_LINES)
    valid_node_children_ids = get_node_descendants(cursors, node_id, False, True)
    content_lines.append(f"{_BACKLINK_LINE_START}({GIT_SEARCH_URL + node_id_to_hex(node_id)})")
    for i, child_id in enumerate(valid_node_children_ids):
        hex_id = node_id_to_hex(child_id)
        content_lines.append(f"{i}. [`{hex_id}`]({hex_id}.md)")
    with open_write_lf(_GIT_FOLDER.joinpath(node_id_to_hex(node_id) + ".md"), False) as f:
        f.write('\n'.join(content_lines) + '\n')
    return valid_node_children_ids


def pop_unsynced_nodes(cursors: Cursors):
    last_sync = LastSync()
    unsynced_children = cursors.unsynced_children
    if unsynced_children.first():
        while True:
            node_id: NodeId = unsynced_children.key().decode()
            unsynced_children.delete()
            children_ids = get_node_descendants(cursors, node_id, False, False)
            if cursors.content.set_key(node_id.encode()):  # Check for invalid nodeId
                content = get_node_content(cursors, node_id)
                last_sync[node_id] = NodeData(content, children_ids)
            if not unsynced_children.next():
                break
    unsynced_content = cursors.unsynced_content
    if unsynced_content.first():
        while True:
            cur_node_id: NodeId = unsynced_content.key().decode()
            unsynced_content.delete()
            content_lines = get_node_content(cursors, cur_node_id)
            if cur_node_id not in last_sync:
                last_sync[cur_node_id] = NodeData(content_lines, OrderedSet())
            if not unsynced_content.next():
                break
    return last_sync


def file_children_line_to_node_id(line: str) -> NodeId:
    uuid_match = search(r"[0-9a-f]{8}(?:-?[0-9a-f]{4}){4}[0-9a-f]{8}(?=\.md\)$)", line)
    assert uuid_match, f"Child node ID for '{line}' couldn't be parsed"
    return file_name_to_node_id(uuid_match.group(), '')


def get_file_content_children(file: TextIO) -> tuple[list[str], OrderedSet]:
    lines = file.read().splitlines()
    children_ids = []
    while lines:
        line = lines.pop()
        if line.startswith(_BACKLINK_LINE_START):
            assert lines.pop() == _CONTENT_CHILDREN_SEPARATOR_LINES[1]
            assert lines.pop() == _CONTENT_CHILDREN_SEPARATOR_LINES[0]
            break
        children_ids.append(file_children_line_to_node_id(line))
    return lines, OrderedSet(reversed(children_ids))


def sigterm_handler(_signal: Signals, _traceback_frame: FrameType) -> None:
    raise SystemExit(1)


class GitInit:
    if getsignal(SIGTERM) == SIG_DFL:
        signal(SIGTERM, sigterm_handler)  # Signal handler (for pid) must be set from main thread

    def __enter__(self) -> None:
        from pid import PidFile, PidFileAlreadyLockedError  # 0.06s
        self.process_lock = PidFile(pidname="qualia_lock", piddir=_GIT_FOLDER.joinpath(".git"),
                                    register_term_signal_handler=False)  # Can't register handler in non-main thread
        retry_count = 10
        for try_num in range(retry_count + 1):
            try:
                self.process_lock.__enter__()
                break
            except PidFileAlreadyLockedError as e:
                if try_num == retry_count:
                    logger.critical("Git sync failed due to failed lock acquisition.")
                    raise e
                sleep(5)
        existing_branch = cd_run_git_cmd(["branch", "--show-current"])
        if existing_branch == GIT_BRANCH:
            self.different_existing_branch = None
        else:
            self.different_existing_branch = existing_branch
            cd_run_git_cmd(["stash"])
            cd_run_git_cmd(["switch", "-c", GIT_BRANCH])

    def __exit__(self, *_args) -> None:
        if self.different_existing_branch:
            cd_run_git_cmd(["checkout", self.different_existing_branch])
            cd_run_git_cmd(["stash", "pop"])
        self.process_lock.__exit__()


class LockNotAcquired(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
