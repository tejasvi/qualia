from __future__ import annotations

from glob import glob
from pathlib import Path
from sys import path, argv

from orderedset import OrderedSet

path.append(Path(__file__).parent.parent.as_posix())  # noqa: E402

from typing import Optional, TYPE_CHECKING

from qualia.config import GIT_BRANCH, GIT_AUTHORIZED_REMOTE, _GIT_FOLDER, \
    _GIT_ENCRYPTION_ENABLED_FILE_NAME
from qualia.models import CustomCalledProcessError, GitChangedNodes, GitMergeError, KeyNotFoundError
from qualia.utils.bootstrap_utils import repository_setup, bootstrap
from qualia.utils.common_utils import cd_run_git_cmd, file_name_to_node_id, logger, \
    exception_traceback, conflict, trigger_buffer_change
from qualia.database import Database
from qualia.services.utils.git_utils import create_markdown_file, repository_file_to_content_children, \
    GitInit

if TYPE_CHECKING:
    from pynvim import Nvim


def sync_with_git(nvim):
    # type:(Optional[Nvim]) -> None
    logger.critical("Git sync started")
    assert repository_setup.wait(60), "Repository setup not yet finished"
    try:
        with GitInit():
            changed_file_names = fetch_from_remote()
            repository_encrypted = _GIT_FOLDER.joinpath(_GIT_ENCRYPTION_ENABLED_FILE_NAME).is_file()
            with Database() as db:
                if changed_file_names:
                    directory_to_db(db, changed_file_names, repository_encrypted)
                    logger.debug("Git Change")
                    if nvim:
                        trigger_buffer_change(nvim)
                db_to_directory(db, repository_encrypted)
            push_to_remote()
    except Exception as e:
        if nvim and isinstance(e, GitMergeError):
            nvim.async_call(
                nvim.err_write(
                    "Merging the new changes in git repository failed. Inspect at " + _GIT_FOLDER.as_posix()))
        logger.critical(
            "Error while syncing with git\n" + exception_traceback(e))
        raise e


def fetch_from_remote() -> list[str]:
    cd_run_git_cmd(["add", "-A"])
    try:
        cd_run_git_cmd(["commit", "-am", "Unknown changes"])
    except CustomCalledProcessError:
        pass
    try:
        cd_run_git_cmd(["fetch", GIT_AUTHORIZED_REMOTE, GIT_BRANCH])
    except CustomCalledProcessError:
        logger.critical("Couldn't fetch")
    else:
        try:
            cd_run_git_cmd(["merge-base", "--is-ancestor", "FETCH_HEAD", "HEAD"])
        except CustomCalledProcessError:
            commit_hash_before_merge = cd_run_git_cmd(["rev-parse", "HEAD"])
            try:
                cd_run_git_cmd(["merge", "FETCH_HEAD"])
            except GitMergeError as exp:
                raise exp
                # if cd_run_git_cmd(["ls-files", "-u"]):
                #     cd_run_git_cmd(["commit", "-A", "Merge  conflicts"])
                # else:
                #     raise exp
            else:
                changed_file_names = cd_run_git_cmd(
                    ["diff", "--name-only", commit_hash_before_merge, "FETCH_HEAD"]).splitlines()
                return changed_file_names
    return []


def push_to_remote() -> None:
    cd_run_git_cmd(["add", "-A"])
    if cd_run_git_cmd(["status", "--porcelain"]):
        cd_run_git_cmd(["commit", "-m", "⎛⎝(='.'=)⎠⎞"])
        try:
            cd_run_git_cmd(["push", "-u", GIT_AUTHORIZED_REMOTE, GIT_BRANCH])
        except CustomCalledProcessError as e:
            logger.critical("Could not push: " + str(e))


def directory_to_db(db: Database, changed_file_names: list[str], repository_encrypted: bool) -> None:
    changed_nodes: GitChangedNodes = {}
    for file_name in changed_file_names:
        relative_file_path = Path(file_name)
        absolute_file_path = _GIT_FOLDER.joinpath(file_name)
        if absolute_file_path.exists() and len(relative_file_path.parts) == 1 and absolute_file_path.is_file():
            try:
                node_id = file_name_to_node_id(relative_file_path.name, ".md")
            except ValueError:
                logger.critical("Invalid ", relative_file_path)
            else:
                with open(absolute_file_path) as f:
                    content_lines, children_ids = repository_file_to_content_children(f, repository_encrypted)
                    changed_nodes[node_id] = OrderedSet(children_ids), content_lines
    logger.debug(f"d2db {changed_file_names} {changed_nodes}")

    sync_git_to_db(changed_nodes, db)


def sync_git_to_db(changed_nodes: GitChangedNodes, db: Database) -> None:
    for cur_node_id, (children_ids, content_lines) in changed_nodes.items():
        if db.pop_if_unsynced_children(cur_node_id):
            db_children_ids = db.get_node_descendants(cur_node_id, False, True)
            children_ids.update(db_children_ids)
        db.set_node_descendants(cur_node_id, children_ids, False)

        if db.pop_if_unsynced_content(cur_node_id):
            try:
                db_content_lines = db.get_node_content_lines(cur_node_id)
            except KeyNotFoundError:
                pass
            else:
                content_lines = conflict(content_lines, db_content_lines)
        db.set_node_content_lines(cur_node_id, content_lines)


def db_to_directory(db: Database, repository_encrypted: bool) -> None:
    existing_markdown_file_paths = glob(_GIT_FOLDER.as_posix() + "/*.md")
    for md_file_path in existing_markdown_file_paths:
        Path(md_file_path).unlink()

    root_id = db.get_root_id()
    visited = {root_id}
    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()
        valid_node_children_ids = create_markdown_file(db, node_id, repository_encrypted)
        db.delete_unsynced_content_children(node_id)
        node_stack.extend(valid_node_children_ids.difference(visited))
        visited.update(valid_node_children_ids)


if __name__ == "__main__" and argv[-1].endswith("git.py"):
    bootstrap()  # Fresh db restoration from repo E.g. Set config then $ python git.py
    sync_with_git(None)

"""
client subscribe to firestore realtime events for realtime changes. rtdb not used for full fetch

bloom filters used for search. Writing node means updating its bloom filter
    Can include context indexing as well
    Counting bloom filter useful
    cuckoo filter not useful since
        deletion not common
        their counting variant implementation is not available
        partial speed is the only advantage
"""
