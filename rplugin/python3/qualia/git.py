from base64 import urlsafe_b64encode
from os import scandir, DirEntry, symlink
from pathlib import Path
from shutil import rmtree
from subprocess import run
from tempfile import gettempdir
from time import sleep
from typing import Iterable
from uuid import UUID

from dulwich import porcelain
from dulwich.repo import Repo
from orderedset import OrderedSet

from qualia import NodeId, states
from qualia.config import GIT_FOLDER, GIT_URL
from qualia.models import ProcessState, Ledger
from qualia.sync import sync_with_db
from qualia.utils import get_key_val


def check_symlinks_enabled() -> bool:
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


symlinks_enabled = check_symlinks_enabled()


class GitLock:
    def __enter__(self) -> None:
        max_tries = 3
        for tries in range(1, max_tries + 1):
            try:
                self.lock_file_path = Path(GIT_FOLDER).joinpath(".git/.qualia_lock")
                self.lock_file = open(self.lock_file_path, 'x')
                break
            except FileExistsError as exp:
                if tries == max_tries:
                    raise exp
                sleep(10)

    def __exit__(self, *_args) -> None:
        self.lock_file.close()
        self.lock_file_path.unlink()


def db_to_git(repo: Repo):
    children_cursor = states.cursors.children
    children_cursor.first()
    root_id_bytes, _ = children_cursor.item()

    root_id: NodeId = root_id_bytes.decode()

    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()

        node_directory_path = Path(GIT_FOLDER).joinpath(node_id + ".q")
        node_directory_path.mkdir()

        node_children_ids: list[NodeId] = get_key_val(root_id, children_cursor)

        add_children_to_node_directory(node_children_ids, node_directory_path)
        add_content_to_node_directory(node_directory_path, node_id)

        node_stack.extend(node_children_ids)

    run(["git", "commit", "-am"],check=True, cwd=GIT_FOLDER)
    porcelain.add(repo)
    porcelain.commit(repo)
    try:
        porcelain.pull(repo)
        porcelain.ls_remote(porcelain.get_branch_remote(porcelain.open_repo(".")))
    except RuntimeError:
        sync_with_git()
    porcelain.push()


def add_content_to_node_directory(node_directory_path: Path, node_id: NodeId):
    content_lines = get_key_val(node_id, states.cursors.content)
    with open(node_directory_path.joinpath("README.md"), 'x') as content_file:
        content_file.writelines(content_lines)


def add_children_to_node_directory(node_children_ids: Iterable[NodeId], node_directory_path: Path):
    for child_node_id in node_children_ids:
        child_path = node_directory_path.joinpath(child_node_id + ".q")
        if symlinks_enabled:
            symlink(f"../{child_node_id}.q", child_path, target_is_directory=True)
        else:
            open(child_path, 'x').close()


def sync_with_git() -> None:
    with GitLock():
        run(["git", "pull", GIT_URL],check=True, cwd=GIT_FOLDER)
        git_to_db()
        db_to_git()
        run(["git", "push", GIT_URL],check=True, cwd=GIT_FOLDER)


def git_to_db() -> None:
    with scandir(GIT_FOLDER) as entries:
        entries: Iterable[DirEntry]
        process_state = ProcessState()
        for entry in entries:
            if entry.is_dir():
                try:
                    store_node_directory_data(entry, process_state)
                    rmtree(entry.path)
                except NotNodeDirectory:
                    pass
        sync_with_db(None, process_state, Ledger())


class NotNodeDirectory(Exception):
    """The directory is invalid node. Should contain README.md and name should be hex encoded UUID"""


def store_node_directory_data(entry, process_state) -> None:
    try:
        node_id = directory_name_to_node_id(entry)
        with scandir(entry.path) as children:
            children: Iterable[DirEntry]
            children_ids = [directory_name_to_node_id(child.name) for child in children if
                            child.name.endswith(".q") and is_valid_uuid(child.name[:-2])]
        with open(entry.path.join("README.md")).read() as content_file:
            content_lines = content_file.splitlines()
        process_state.changed_children_map[node_id] = OrderedSet(children_ids)
        process_state.changed_content_map[node_id] = content_lines
    except (ValueError, FileNotFoundError):
        raise NotNodeDirectory


def is_valid_uuid(string: str) -> bool:
    try:
        UUID(string)
    except ValueError:
        return False
    return True


def directory_name_to_node_id(directory: DirEntry) -> NodeId:
    directory_name = directory.name
    if directory_name.endswith(".md"):
        node_id_hex = directory_name[:2]
    else:
        raise ValueError
    node_id = NodeId(urlsafe_b64encode(UUID(node_id_hex)).decode())
    return node_id
