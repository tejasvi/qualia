from threading import Event
from typing import cast

from orderedset import OrderedSet

from qualia.config import GIT_BRANCH, GIT_AUTHORIZED_REMOTE, _GIT_FOLDER, _ROOT_ID_KEY, ENCRYPT_DB, \
    _DB_ENCRYPTION_ENABLED_KEY, ENCRYPT_NEW_GIT_REPOSITORY, _GIT_ENCRYPTION_ENABLED_FILE_NAME, \
    _GIT_ENCRYPTION_DISABLED_FILE_NAME
from qualia.models import Cursors, DbClient, CustomCalledProcessError, NodeId, Li, El
from qualia.services.backup import backup_db
from qualia.services.listener import RpcListenExternal
from qualia.utils.common_utils import cd_run_git_cmd, exception_traceback, StartLoggedThread, get_set_client, \
    set_node_content_lines, _set_node_descendants_value, open_write_lf, decrypt_lines, get_db_node_content_lines, \
    cursor_keys, set_node_descendants
from qualia.utils.common_utils import logger, get_key_val, set_key_val, Database


def ensure_root_node(cursors: Cursors) -> None:
    if get_key_val(_ROOT_ID_KEY, cursors.metadata, False) is None:
        root_id = cast(NodeId, "017b99da-b1b5-19e9-e98d-8584cf46cfcf")  # get_time_uuid()
        set_node_content_lines(root_id, cast(Li, ['']), cursors)
        set_node_descendants(root_id,OrderedSet(), cursors,  False)
        set_key_val(_ROOT_ID_KEY, root_id, cursors.metadata, False)


repository_setup = Event()


def setup_repository(client_data: DbClient) -> None:
    try:
        cd_run_git_cmd(["rev-parse", "--is-inside-work-tree"])
    except CustomCalledProcessError:
        cd_run_git_cmd(["init"])
        cd_run_git_cmd(["checkout", "-b", GIT_BRANCH])
        if GIT_AUTHORIZED_REMOTE:
            try:
                logger.debug("Fetching repository")
                cd_run_git_cmd(["fetch", GIT_AUTHORIZED_REMOTE, GIT_BRANCH])
                cd_run_git_cmd(["merge", "FETCH_HEAD"])
            except CustomCalledProcessError as e:
                logger.critical(f"Can't fetch and merge from {GIT_BRANCH}.\nError: " + exception_traceback(e))
                raise e

        gitattributes_path = _GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            with open_write_lf(gitattributes_path, True) as f:
                f.write("*.md merge=union\n* text=auto eol=lf\n")
            cd_run_git_cmd(["add", "-A"])
            cd_run_git_cmd(["commit", "-m", "Bootstrap"])

        encryption_enabled_file = _GIT_FOLDER.joinpath(_GIT_ENCRYPTION_ENABLED_FILE_NAME)
        encryption_disabled_file = _GIT_FOLDER.joinpath(_GIT_ENCRYPTION_DISABLED_FILE_NAME)
        if not (encryption_enabled_file.exists() or encryption_disabled_file.exists()):
            (encryption_enabled_file if ENCRYPT_NEW_GIT_REPOSITORY else encryption_disabled_file).touch()

        cd_run_git_cmd(["config", "user.name", client_data["client_name"]])
        cd_run_git_cmd(["config", "user.email", f""])  # {client_data['client_id']}@q.client"])

    repository_setup.set()


def setup_encryption(cursors: Cursors) -> None:
    db_encrypted = get_key_val(_DB_ENCRYPTION_ENABLED_KEY, cursors.metadata, False)
    if bool(ENCRYPT_DB) != bool(db_encrypted):
        for node_id in cursor_keys(cursors.content):
            node_id = cast(NodeId, node_id)
            db_content_lines = get_db_node_content_lines(cursors, node_id)
            if ENCRYPT_DB:
                content_lines = cast(Li, db_content_lines)
            else:
                db_content_lines = cast(El, db_content_lines)
                content_lines = decrypt_lines(db_content_lines)
            set_node_content_lines(node_id, content_lines, cursors)
        for _node_id in cursor_keys(cursors.bloom_filters):
            cursors.bloom_filters.delete()
        set_key_val(_DB_ENCRYPTION_ENABLED_KEY, bool(ENCRYPT_DB), cursors.metadata, True)


def bootstrap() -> None:
    with Database() as cursors:
        setup_encryption(cursors)
        client_data = get_set_client(cursors.metadata)
        StartLoggedThread(target=lambda: setup_repository(client_data), name="SetupRepo")
        ensure_root_node(cursors)
    StartLoggedThread(target=backup_db, name="BackupDB")
    StartLoggedThread(target=RpcListenExternal, name="RPClistener")
