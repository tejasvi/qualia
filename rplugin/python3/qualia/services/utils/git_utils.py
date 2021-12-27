from functools import cache
from os import symlink
from pathlib import Path
from re import search
from signal import getsignal, SIGTERM, SIG_DFL, signal, Signals
from tempfile import gettempdir
from types import FrameType
from typing import Iterable, cast

from orderedset import OrderedSet

from qualia.config import GIT_SEARCH_URL, _GIT_DATA_FOLDER, GIT_BRANCH, _SORT_SIBLINGS
from qualia.models import NodeId, El, Li, InvalidFileChildrenLine, MinimalDb
from qualia.utils.common_utils import cd_run_git_cmd, open_write_lf, decrypt_lines, encrypt_lines, acquire_process_lock

_CONTENT_CHILDREN_SEPARATOR_LINES = ["<hr>", ""]


def add_children_to_node_directory(node_children_ids: Iterable[NodeId], node_directory_path: Path):
    for child_node_id in node_children_ids:
        child_path = node_directory_path.joinpath(child_node_id + ".q")
        symlink_source = f"../{child_node_id}.q"
        if symlinks_enabled():
            symlink(symlink_source, child_path, target_is_directory=True)
        else:
            # TODO: verify if symlink files need newlines at the end
            open_write_lf(child_path, True, [symlink_source])


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


def create_markdown_file(db: MinimalDb, node_id: NodeId, repository_encrypted: bool) -> None:
    """
    CONTENT
    CONTENT ...
    <Any line> - backlink search link in this case. Below is empty line.

    Line containing child's "<UUID>.md"
    Line containing child's "<UUID>.md" ...
    """
    content_lines = db.get_node_content_lines(node_id, temporary)
    markdown_file_lines = encrypt_lines(content_lines) if repository_encrypted else content_lines
    valid_node_children_ids = db.get_node_descendants(node_id, False, True, temporary)
    markdown_file_lines.append(f"<hr><ol start=0><li><a href='{GIT_SEARCH_URL + node_id}+md'>Backlinks</a></li></ol>)")
    markdown_file_lines.append("")
    for i, child_id in enumerate(sorted(valid_node_children_ids) if _SORT_SIBLINGS else valid_node_children_ids):
        markdown_file_lines.append(f"{i + 1}. [`{child_id}`]({child_id}.md)")
    open_write_lf(node_git_filepath(node_id), False, markdown_file_lines)


def node_git_filepath(node_id: NodeId) -> Path:
    return _GIT_DATA_FOLDER.joinpath(node_id + ".md")


def file_children_line_to_node_id(line: str) -> NodeId:
    # TODO: Case sensitivity?
    uuid_match = search(r"[0-9a-f]{8}(?:-?[0-9a-f]{4}){4}[0-9a-f]{8}(?=\.md\)$)", line)
    if not uuid_match:
        raise InvalidFileChildrenLine(f"Child node ID for '{line}' couldn't be parsed")
    return cast(NodeId, uuid_match.group())


def repository_file_to_content_children(file_path: Path, encrypted: bool) -> tuple[Li, OrderedSet]:
    with open(file_path) as file:
        lines = file.read().splitlines()
        children_ids = []

        while lines:
            line = lines.pop()
            if line:
                children_ids.append(file_children_line_to_node_id(line))
            else:
                lines.pop()
                break

        lines = decrypt_lines(cast(El, lines)) if encrypted else cast(Li, lines)
        return lines, OrderedSet(reversed(children_ids))


def sigterm_handler(_signal: Signals, _traceback_frame: FrameType) -> None:
    raise SystemExit(1)


class GitInit:
    if getsignal(SIGTERM) == SIG_DFL:
        signal(SIGTERM, sigterm_handler)  # Signal handler (for pid) must be set from main thread

    def __init__(self, git_root_dir: Path):
        self.git_root_dir = git_root_dir

    def __enter__(self) -> None:
        git_root_dir = self.git_root_dir

        self.acquired_process_lock = acquire_process_lock("qualia.lock", git_root_dir.joinpath(".git"), 10, 5)

        existing_branch = cd_run_git_cmd(["branch", "--show-current"], git_root_dir)
        if existing_branch == GIT_BRANCH:
            self.different_existing_branch = None
        else:
            self.different_existing_branch = existing_branch
            cd_run_git_cmd(["stash"], git_root_dir)
            cd_run_git_cmd(["switch", "-c", GIT_BRANCH], git_root_dir)

    def __exit__(self, *_args) -> None:
        if self.different_existing_branch:
            cd_run_git_cmd(["checkout", self.different_existing_branch], self.git_root_dir)
            cd_run_git_cmd(["stash", "pop"], self.git_root_dir)
        self.acquired_process_lock.__exit__()


class LockNotAcquired(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
