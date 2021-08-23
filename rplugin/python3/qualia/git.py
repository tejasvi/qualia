from __future__ import annotations

from pathlib import Path
from subprocess import CalledProcessError
from typing import cast

from qualia.config import ROOT_ID_KEY, GIT_BRANCH, GIT_TOKEN_URL
from qualia.models import ProcessState, Cursors, NodeId
from qualia.sync import sync_with_db
from qualia.utils import get_key_val, run_git_cmd, GitInit, Database, name_to_node_id, get_file_content_children, \
    pop_unsynced_nodes, \
    create_markdown_file, repository_exists


def sync_with_git() -> None:
    assert repository_exists.wait(timeout=60), "Git repository does not exist for syncing"
    with GitInit(), Database() as cursors:
        changed_file_names = fetch_from_remote()
        directory_to_db(cursors, changed_file_names)
        db_to_directory(cursors)
        push_to_remote()


def fetch_from_remote() -> list[str]:
    run_git_cmd(["add", "-A"])
    try:
        run_git_cmd(["commit", "-am", "Unknown changes"])
    except CalledProcessError:
        pass
    try:
        print(run_git_cmd(["fetch", GIT_TOKEN_URL, GIT_BRANCH]))
    except CalledProcessError:
        print("Couldn't fetch")
        return []
    else:
        commit_has_before_merge = run_git_cmd(["rev-parse", "HEAD"])
        try:
            print(run_git_cmd(["merge", "FETCH_HEAD"]))
        except CalledProcessError as exp:
            raise exp
            # if run_git_cmd(["ls-files", "-u"]):
            #     run_git_cmd(["commit", "-A", "Merge  conflicts"])
            # else:
            #     raise exp
        else:
            changed_file_names = run_git_cmd(
                ["diff", "--name-only", commit_has_before_merge, "FETCH_HEAD"]).splitlines()
            return changed_file_names


def push_to_remote() -> None:
    run_git_cmd(["add", "-A"])
    if run_git_cmd(["status", "--porcelain"]):
        run_git_cmd(["commit", "-m", "Vim"])
        try:
            run_git_cmd(["push", "-u", GIT_TOKEN_URL, GIT_BRANCH])
        except CalledProcessError:
            print("Could not push")


def directory_to_db(cursors: Cursors, changed_file_names: list[str]) -> None:
    process_state = ProcessState()
    for file_name in changed_file_names:
        path = Path(file_name)
        if path.exists() and len(path.parts) == 1 and path.is_file():
            try:
                node_id = name_to_node_id(path.name, ".q.md")
            except ValueError:
                print("Invalid ", path)
            else:
                with open(path) as f:
                    content_lines, children_ids = get_file_content_children(f)
                    process_state.changed_children_map[node_id] = children_ids
                    process_state.changed_content_map[node_id] = content_lines

    if process_state:
        last_seen = pop_unsynced_nodes(cursors)
        # Realtime broadcast not done since git conflict, if any, will eventually surface
        sync_with_db(None, process_state, last_seen, cursors, False, False)


def db_to_directory(cursors: Cursors) -> None:
    root_id = cast(NodeId, get_key_val(ROOT_ID_KEY, cursors.metadata))
    node_stack = [root_id]
    while node_stack:
        node_id = node_stack.pop()
        node_children_ids = create_markdown_file(cursors, node_id)
        node_stack.extend(node_children_ids)


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
