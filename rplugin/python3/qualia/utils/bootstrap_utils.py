from pathlib import Path
from threading import Event
from typing import cast

from orderedset import OrderedSet

from qualia.config import GIT_BRANCH, GIT_AUTHORIZED_REMOTE, ENCRYPT_DB, \
    ENCRYPT_NEW_GIT_REPOSITORY, _GIT_ENCRYPTION_ENABLED_FILE_NAME, \
    _GIT_ENCRYPTION_DISABLED_FILE_NAME, _GIT_FOLDER
from qualia.database import Database, MuDatabase
from qualia.models import DbClient, CustomCalledProcessError, NodeId, Li, KeyNotFoundError, SourceId, SourceShortId
from qualia.services.backup import backup_db
from qualia.services.listener import RpcListenExternal
from qualia.utils.common_utils import cd_run_git_cmd, exception_traceback, StartLoggedThread, open_write_lf, get_uuid
from qualia.utils.common_utils import live_logger






def setup_repository(repository_dir: Path, setup_event: Event) -> None:
    try:
        cd_run_git_cmd(["rev-parse", "--is-inside-work-tree"], repository_dir)
    except CustomCalledProcessError:
        cd_run_git_cmd(["init"], repository_dir)
        cd_run_git_cmd(["checkout", "-b", GIT_BRANCH], repository_dir)
        if GIT_AUTHORIZED_REMOTE:
            try:
                live_logger.debug("Fetching repository")
                cd_run_git_cmd(["fetch", GIT_AUTHORIZED_REMOTE, GIT_BRANCH], repository_dir)
            except CustomCalledProcessError as e:
                live_logger.debug(f"Can't fetch and merge from {GIT_BRANCH}.\nError: " + exception_traceback(e))
            else:
                try:
                    cd_run_git_cmd(["merge", "FETCH_HEAD"], repository_dir)
                except CustomCalledProcessError as e:
                    live_logger.critical(f"Can't merge with fetched data.\nError: " + exception_traceback(e))
                    raise e

        gitattributes_path = _GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            open_write_lf(gitattributes_path, True, ["*.md merge=union", "* text=auto eol=lf"])
            cd_run_git_cmd(["add", "-A"], repository_dir)
            cd_run_git_cmd(["commit", "-m", "Bootstrap"], repository_dir)

        encryption_enabled_file = _GIT_FOLDER.joinpath(_GIT_ENCRYPTION_ENABLED_FILE_NAME)
        encryption_disabled_file = _GIT_FOLDER.joinpath(_GIT_ENCRYPTION_DISABLED_FILE_NAME)
        if not (encryption_enabled_file.exists() or encryption_disabled_file.exists()):
            (encryption_enabled_file if ENCRYPT_NEW_GIT_REPOSITORY else encryption_disabled_file).touch()

        cd_run_git_cmd(["config", "user.name", source_name], repository_dir)
        cd_run_git_cmd(["config", "user.email", f""], repository_dir)

    setup_event.set()


def setup_encryption(db: MuDatabase) -> None:


def bootstrap() -> None:
    with Database.main_db() as db:
    StartLoggedThread(target=backup_db, name="BackupDB", delay_seconds=2)
