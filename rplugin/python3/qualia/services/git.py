from __future__ import annotations

from pathlib import Path
from sys import path, argv
from uuid import UUID

from orderedset import OrderedSet

path.append(Path(__file__).parent.parent.as_posix())  # noqa: E402

from typing import Optional, TYPE_CHECKING, cast

from qualia.config import GIT_BRANCH, GIT_AUTHORIZED_REMOTE, _GIT_DATA_FOLDER, \
    _GIT_ENCRYPTION_ENABLED_FILE_NAME, _GIT_FOLDER, ENABLE_GIT_SYNC
from qualia.models import CustomCalledProcessError, GitChangedNodes, GitMergeError, KeyNotFoundError, NodeId, \
    InvalidFileChildrenLine
from qualia.utils.bootstrap_utils import repository_setup, bootstrap
from qualia.utils.common_utils import cd_run_git_cmd, file_name_to_file_id, live_logger, \
    exception_traceback, conflict, trigger_buffer_change
from qualia.database import Database
from qualia.services.utils.git_utils import create_markdown_file, repository_file_to_content_children, \
    GitInit, node_git_filepath

if TYPE_CHECKING:
    from pynvim import Nvim


def sync_with_git(nvim):
    # type:(Optional[Nvim]) -> None
    """
    Invariant: State of git repository is synced with DB before starting git sync.
    """
    if not ENABLE_GIT_SYNC:
        return
    live_logger.debug("Git sync started")
    if repository_setup.wait(60):
        live_logger.error("Repository setup not yet finished")
        return
    try:
        with GitInit():
            changed_file_names = fetch_from_remote()
            repository_encrypted = _GIT_DATA_FOLDER.joinpath(_GIT_ENCRYPTION_ENABLED_FILE_NAME).is_file()
            with Database() as db:
                if changed_file_names:
                    directory_to_db(db, changed_file_names, repository_encrypted)
                    live_logger.debug("Git Change")
                    if nvim:
                        trigger_buffer_change(nvim)
                db_to_directory(db, repository_encrypted)
            push_to_remote()
    except Exception as e:
        if nvim and isinstance(e, GitMergeError):
            nvim.async_call(
                nvim.err_write(
                    "Merging the new changes in git repository failed. Inspect at " + _GIT_FOLDER.as_posix()))
        live_logger.critical(
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
        live_logger.debug("Couldn't fetch")
    else:
        try:
            cd_run_git_cmd(["merge-base", "--is-ancestor", "FETCH_HEAD", "HEAD"])
        except CustomCalledProcessError:
            try:
                commit_hash_before_merge = cd_run_git_cmd(["rev-parse", "HEAD"])
            except CustomCalledProcessError:
                commit_hash_before_merge = None
            try:
                cd_run_git_cmd(["merge", "FETCH_HEAD", "--allow-unrelated-histories"])
            except GitMergeError as exp:
                raise exp
                # Auto commit merge conflicts?
                # if cd_run_git_cmd(["ls-files", "-u"]):
                #     cd_run_git_cmd(["commit", "-A", "Merge  conflicts"])
                # else:
                #     raise exp
            else:
                changed_file_names = _GIT_DATA_FOLDER.glob("*.md") if commit_hash_before_merge is None else cd_run_git_cmd(
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
            live_logger.debug("Could not push: " + str(e))


def directory_to_db(db: Database, changed_file_names: list[str], repository_encrypted: bool) -> None:
    changed_nodes: GitChangedNodes = {}
    for file_name in changed_file_names:
        relative_file_path = Path(file_name)
        absolute_file_path = _GIT_DATA_FOLDER.joinpath(file_name)
        if absolute_file_path.exists() and len(relative_file_path.parts) == 1 and absolute_file_path.is_file():
            try:
                file_id = file_name_to_file_id(relative_file_path.name, ".md")
                UUID(file_id)
                node_id = cast(NodeId, file_id)
            except ValueError:
                live_logger.critical(f"Invalid {relative_file_path}")
            else:
                try:
                    content_lines, children_ids = repository_file_to_content_children(absolute_file_path, repository_encrypted)
                except InvalidFileChildrenLine as e:
                    live_logger.critical(
                        f"{file_name} is in invalid format. Could not extract it's content and children.")
                    raise e
                changed_nodes[node_id] = OrderedSet(children_ids), content_lines

    sync_git_to_db(changed_nodes, db)


def sync_git_to_db(changed_nodes: GitChangedNodes, db: Database) -> None:
    for cur_node_id, (children_ids, content_lines) in changed_nodes.items():
        if db.if_unsynced_children(cur_node_id):
            db_children_ids = db.get_node_descendants(cur_node_id, False, True)
            children_ids.update(db_children_ids)
        db.set_node_descendants(cur_node_id, children_ids, False)

        if db.if_unsynced_content(cur_node_id):
            try:
                db_content_lines = db.get_node_content_lines(cur_node_id)
            except KeyNotFoundError:
                pass
            else:
                content_lines = conflict(content_lines, db_content_lines)
        db.set_node_content_lines(cur_node_id, content_lines)


def db_to_directory(db: Database, repository_encrypted: bool) -> None:
    modified_node_ids = set()
    for node_id in db.pop_unsynced_node_ids():
        if db.is_valid_node(node_id):
            modified_node_ids.add(node_id)
        else:
            node_git_filepath(node_id).unlink(missing_ok=True)
            parents = db.get_node_descendants(node_id, True, True)
            modified_node_ids.update(parents)
    for node_id in modified_node_ids:
        create_markdown_file(db, node_id, repository_encrypted)


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
