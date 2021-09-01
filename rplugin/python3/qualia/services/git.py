from __future__ import annotations

from glob import glob
from pathlib import Path
from sys import path

from orderedset import OrderedSet
from pynvim import Nvim

path.append(Path(__file__).parent.parent.as_posix())  # noqa: E402

from typing import cast, Optional

from qualia.config import _ROOT_ID_KEY, GIT_BRANCH, GIT_TOKEN_URL, _GIT_FOLDER
from qualia.models import Cursors, NodeId, CustomCalledProcessError, GitChangedNodes, GitMergeError
from qualia.utils.bootstrap_utils import repository_setup, bootstrap
from qualia.utils.common_utils import cd_run_git_cmd, Database, file_name_to_node_id, get_key_val, logger, \
    exception_traceback, get_node_descendants, set_ancestor_descendants, conflict, set_node_content_lines
from qualia.services.utils.git_utils import create_markdown_file, get_file_content_children, \
    GitInit


def sync_with_git(nvim: Optional[Nvim]) -> None:
    logger.critical("Git sync started")
    assert repository_setup.wait(60), "Repository setup not yet finished"
    try:
        with GitInit():
            changed_file_names = fetch_from_remote()
            with Database() as cursors:
                if changed_file_names:
                    directory_to_db(cursors, changed_file_names)
                    # if os.name == 'nt':
                    #     nvim.command("normal vyvp", async_=True)
                    # else:
                    #     nvim.async_call(nvim.command, "normal vyvp", async_=True)
                    logger.debug("Git Change")
                    nvim.async_call(nvim.command, "normal vyvp", async_=True)
                db_to_directory(cursors)
            push_to_remote()
    except Exception as e:
        if nvim and isinstance(e, GitMergeError):
            nvim.async_call(
                nvim.err_write("Merging the new changes in git repository failed. Inspect at " + _GIT_FOLDER))
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
            cd_run_git_cmd(["push", "-u", GIT_TOKEN_URL, GIT_BRANCH])
        except CustomCalledProcessError as e:
            logger.critical("Could not push: " + str(e))


def directory_to_db(cursors: Cursors, changed_file_names: list[str]) -> None:
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
                    content_lines, children_ids = get_file_content_children(f)
                    changed_nodes[node_id] = OrderedSet(children_ids), content_lines
    logger.debug(f"d2db {changed_file_names} {changed_nodes}")

    sync_git_to_db(changed_nodes, cursors)


def sync_git_to_db(changed_nodes: GitChangedNodes, cursors: Cursors) -> None:
    for cur_node_id, (children_ids, content_lines) in changed_nodes.items():
        if cursors.unsynced_children.set_key(cur_node_id.encode()):
            db_children_ids = get_node_descendants(cursors, cur_node_id, False, True)
            children_ids.update(db_children_ids)
            cursors.unsynced_children.delete()
        set_ancestor_descendants(cursors, children_ids, cur_node_id, False)

        logger.debug(f"{changed_nodes} {cursors.unsynced_content.set_key(cur_node_id.encode())}")
        if cursors.unsynced_content.set_key(cur_node_id.encode()):
            db_content_lines = cast(Optional[list], get_key_val(cur_node_id, cursors.content, False))
            if db_content_lines is not None:
                content_lines = conflict(content_lines, db_content_lines)
            cursors.unsynced_content.delete()
        set_node_content_lines(content_lines, cursors, cur_node_id)


def db_to_directory(cursors: Cursors) -> None:
    existing_markdown_file_paths = glob(_GIT_FOLDER.as_posix() + "/*.md")
    for md_file_path in existing_markdown_file_paths:
        Path(md_file_path).unlink()

    root_id = cast(NodeId, get_key_val(_ROOT_ID_KEY, cursors.metadata, True))
    visited = {root_id}
    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()
        valid_node_children_ids = create_markdown_file(cursors, node_id)
        for cursor in (cursors.unsynced_content, cursors.unsynced_children):
            if cursor.set_key(node_id.encode()):
                cursor.delete()
        node_stack.extend(valid_node_children_ids.difference(visited))
        visited.update(valid_node_children_ids)


if __name__ == "__main__":
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
