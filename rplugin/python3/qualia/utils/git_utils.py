from os import symlink
from pathlib import Path
from re import search
from tempfile import gettempdir
from time import sleep
from typing import Iterable, TextIO

from orderedset import OrderedSet

from qualia.config import _CONTENT_CHILDREN_SEPARATOR_LINES, GIT_SEARCH_URL, _GIT_FOLDER, GIT_BRANCH
from qualia.models import NodeData, NodeId, Cursors, LastSeen
from qualia.utils.common_utils import cd_run_git_cmd, node_id_to_hex, get_key_val, file_name_to_node_id


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


symlinks_enabled = _check_symlinks_enabled()


def create_markdown_file(cursors: Cursors, node_id: NodeId) -> list[NodeId]:
    content_lines: list[str] = get_key_val(node_id, cursors.content)
    content_lines.extend(_CONTENT_CHILDREN_SEPARATOR_LINES)
    node_children_ids: list[NodeId] = get_key_val(node_id, cursors.children) or []
    content_lines.append(f"0. [`Backlinks`]({GIT_SEARCH_URL + node_id_to_hex(node_id)})")
    for i, child_id in enumerate(node_children_ids):
        hex_id = node_id_to_hex(child_id)
        content_lines.append(f"{i}. [`{hex_id}`]({hex_id}.md)")
    with open(_GIT_FOLDER.joinpath(node_id_to_hex(node_id) + ".md"), 'w') as f:
        f.write('\n'.join(content_lines) + '\n')
    return node_children_ids


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


def file_children_line_to_node_id(line: str) -> NodeId:
    uuid_match = search(r"[0-9a-f]{8}(?:-?[0-9a-f]{4}){4}[0-9a-f]{8}(?=\.md\)$)", line)
    assert uuid_match, f"Child node ID for '{line}' couldn't be parsed"
    return file_name_to_node_id(uuid_match.group(), '')


def get_file_content_children(file: TextIO) -> tuple[list[str], OrderedSet]:
    lines = file.read().splitlines()
    children_ids = []
    while lines:
        line = lines.pop()
        if line == _CONTENT_CHILDREN_SEPARATOR_LINES[1]:
            assert lines.pop() == _CONTENT_CHILDREN_SEPARATOR_LINES[0]
            break
        children_ids.append(file_children_line_to_node_id(line))
    return lines, OrderedSet(reversed(children_ids))


class GitInit:
    def __enter__(self) -> None:
        assert _GIT_FOLDER.joinpath(".git").exists(), f"{_GIT_FOLDER.joinpath('.git')} does not exist"
        max_tries = 3
        for tries in range(1, max_tries + 1):
            self.lock_file_path = Path(_GIT_FOLDER).joinpath(".git/.qualia_lock")
            try:
                self.lock_file = open(self.lock_file_path, 'x')
            except FileExistsError:
                if tries == max_tries:
                    raise LockNotAcquired(
                        "Could not acquire lock probably due to previous program crash. "
                        f"Verify the data and then delete the Lock File: '{self.lock_file_path}' manually.")
                sleep(10)
            else:
                existing_branch = cd_run_git_cmd(["branch", "--show-current"])
                if existing_branch == GIT_BRANCH:
                    self.existing_branch = None
                else:
                    self.existing_branch = existing_branch
                    cd_run_git_cmd(["stash"])
                    cd_run_git_cmd(["switch", "-c", GIT_BRANCH])
                break

    def __exit__(self, *_args) -> None:
        if self.existing_branch:
            cd_run_git_cmd(["checkout", self.existing_branch])
            cd_run_git_cmd(["stash", "pop"])
        self.lock_file.close()
        self.lock_file_path.unlink()


class LockNotAcquired(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
