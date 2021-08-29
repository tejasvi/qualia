from __future__ import annotations

from pathlib import Path
from sys import path

path.append(Path(__file__).parent.parent.as_posix())  # noqa: E402

from typing import cast

from qualia.config import _ROOT_ID_KEY, GIT_BRANCH, GIT_TOKEN_URL
from qualia.models import ProcessState, Cursors, NodeId, CustomCalledProcessError
from qualia.utils.bootstrap_utils import bootstrap, repository_setup
from qualia.utils.common_utils import cd_run_git_cmd, Database, file_name_to_node_id, get_key_val, logger, \
    exception_traceback, sync_with_db
from qualia.services.utils.git_utils import create_markdown_file, pop_unsynced_nodes, get_file_content_children, \
    GitInit


def sync_with_git() -> None:
    logger.critical("Git sync started")
    assert repository_setup.wait(60), "Repository setup not yet finished"
    try:
        with GitInit():
            changed_file_names = fetch_from_remote()
            with Database() as cursors:
                directory_to_db(cursors, changed_file_names)
                db_to_directory(cursors)
            push_to_remote()
    except Exception as e:
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
        cd_run_git_cmd(["fetch", GIT_TOKEN_URL, GIT_BRANCH])
    except CustomCalledProcessError:
        logger.critical("Couldn't fetch")
        return []
    else:
        commit_has_before_merge = cd_run_git_cmd(["rev-parse", "HEAD"])
        try:
            cd_run_git_cmd(["merge", "FETCH_HEAD"])
        except CustomCalledProcessError as exp:
            raise exp
            # if cd_run_git_cmd(["ls-files", "-u"]):
            #     cd_run_git_cmd(["commit", "-A", "Merge  conflicts"])
            # else:
            #     raise exp
        else:
            changed_file_names = cd_run_git_cmd(
                ["diff", "--name-only", commit_has_before_merge, "FETCH_HEAD"]).splitlines()
            return changed_file_names


def push_to_remote() -> None:
    cd_run_git_cmd(["add", "-A"])
    if cd_run_git_cmd(["status", "--porcelain"]):
        cd_run_git_cmd(["commit", "-m", "Vim"])
        try:
            cd_run_git_cmd(["push", "-u", GIT_TOKEN_URL, GIT_BRANCH])
        except CustomCalledProcessError as e:
            logger.critical("Could not push: " + str(e))


def directory_to_db(cursors: Cursors, changed_file_names: list[str]) -> None:
    process_state = ProcessState()
    for file_name in changed_file_names:
        file_path = Path(file_name)
        if file_path.exists() and len(file_path.parts) == 1 and file_path.is_file():
            try:
                node_id = file_name_to_node_id(file_path.name, ".q.md")
            except ValueError:
                logger.critical("Invalid ", file_path)
            else:
                with open(file_path) as f:
                    content_lines, children_ids = get_file_content_children(f)
                    process_state.changed_descendants_map[node_id] = children_ids
                    process_state.changed_content_map[node_id] = content_lines

    if process_state:
        last_seen = pop_unsynced_nodes(cursors)
        # Realtime broadcast not done since git conflict, if any, will eventually surface
        sync_with_db(None, process_state, last_seen, cursors, False, False)


def db_to_directory(cursors: Cursors) -> None:
    root_id = cast(NodeId, get_key_val(_ROOT_ID_KEY, cursors.metadata, True))
    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()
        node_children_ids = create_markdown_file(cursors, node_id)
        node_stack.extend(node_children_ids)


if __name__ == "__main__":
    bootstrap()  # Fresh db restoration from repo E.g. Set config then $ python git.py
    sync_with_git()

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
