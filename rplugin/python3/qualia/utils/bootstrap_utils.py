from threading import Event
from typing import cast

from orderedset import OrderedSet

from qualia.config import GIT_BRANCH, GIT_AUTHORIZED_REMOTE, _GIT_FOLDER, ENCRYPT_DB, \
    ENCRYPT_NEW_GIT_REPOSITORY, _GIT_ENCRYPTION_ENABLED_FILE_NAME, \
    _GIT_ENCRYPTION_DISABLED_FILE_NAME
from qualia.database import Database
from qualia.models import DbClient, CustomCalledProcessError, NodeId, Li, KeyNotFoundError
from qualia.services.backup import backup_db
from qualia.services.listener import RpcListenExternal
from qualia.utils.common_utils import cd_run_git_cmd, exception_traceback, StartLoggedThread, open_write_lf
from qualia.utils.common_utils import logger


def ensure_root_node(db: Database) -> None:
    try:
        db.get_root_id()
    except KeyNotFoundError:
        root_id = cast(NodeId, "017b99da-b1b5-19e9-e98d-8584cf46cfcf")  # get_time_uuid()
        db.set_node_content_lines(root_id, cast(Li, ['']))
        db.set_node_descendants(root_id, OrderedSet(), False)
        db.set_root_id(root_id)


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


def setup_encryption(db: Database) -> None:
    if bool(ENCRYPT_DB) != db.db_encrypted():
        db.toggle_encryption()


def bootstrap() -> None:
    with Database() as db:
        setup_encryption(db)
        client_data = db.get_set_client()
        StartLoggedThread(target=lambda: setup_repository(client_data), name="SetupRepo")
        ensure_root_node(db)
    StartLoggedThread(target=backup_db, name="BackupDB")
    StartLoggedThread(target=RpcListenExternal, name="RPClistener")
