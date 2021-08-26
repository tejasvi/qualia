import logging
from base64 import urlsafe_b64encode
from datetime import datetime
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from secrets import token_urlsafe
from subprocess import CalledProcessError
from threading import Thread
from time import time
from typing import cast
from uuid import uuid4

from lmdb import Cursor

from qualia.config import GIT_BRANCH, GIT_TOKEN_URL, _GIT_FOLDER, _CLIENT_KEY, _FILE_FOLDER, _ROOT_ID_KEY, \
    _APP_FOLDER_PATH, _DB_FOLDER, _LOG_FILENAME, DEBUG, GIT_REPOSITORY
from qualia.models import Cursors, Client, NodeId
from qualia.utils.common_utils import cd_run_git_cmd
from qualia.utils.common_utils import get_time_uuid, logger, get_key_val, put_key_val, Database


def ensure_root_node(cursors: Cursors) -> None:
    if get_key_val(_ROOT_ID_KEY, cursors.metadata) is None:
        root_id = get_time_uuid()
        put_key_val(root_id, [''], cursors.content, False)
        put_key_val(root_id, [], cursors.children, False)
        put_key_val(root_id, [], cursors.parents, False)
        put_key_val(_ROOT_ID_KEY, root_id, cursors.metadata, False)


def setup_repository(client_data: Client) -> None:
    try:
        cd_run_git_cmd(["rev-parse", "--is-inside-work-tree"])
    except CalledProcessError:
        cd_run_git_cmd(["init"])
        cd_run_git_cmd(["checkout", "-b", GIT_BRANCH])
        try:
            logger.debug("Pulling repository")
            request_time = time()
            cd_run_git_cmd(["pull", GIT_TOKEN_URL, GIT_BRANCH])
            logger.debug(f"Pull took {time() - request_time} seconds")
        except CalledProcessError as e:
            logger.critical(f"Can't fetch from {GIT_REPOSITORY}:{GIT_BRANCH}\nError: {repr(e)}")
        gitattributes_path = _GIT_FOLDER.joinpath(".gitattributes")
        if not gitattributes_path.exists():
            with open(gitattributes_path, 'x') as f:
                f.write("*.md merge=union\n* text=auto eol=lf\n")
            cd_run_git_cmd(["add", "-A"])
            cd_run_git_cmd(["commit", "-m", "bootstrap"])
        cd_run_git_cmd(["config", "user.name", client_data["client_name"]])
        cd_run_git_cmd(["config", "user.email", f"{client_data['client_id']}@q.client"])


def set_client_if_new(metadata_cursor: Cursor):
    if metadata_cursor.get(_CLIENT_KEY.encode()) is None:
        client_details = Client(client_id=str(get_uuid()), client_name=f"Vim-{token_urlsafe(1)}")
        put_key_val(_CLIENT_KEY, client_details, metadata_cursor, False)


def create_directory_if_absent(directory_path: Path):
    try:
        directory_path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (directory_path.is_symlink() and directory_path.is_dir()):
            raise Exception(f"\nMove {directory_path} file to some other location.\n\n")


def bootstrap() -> None:
    for path in (_APP_FOLDER_PATH, _FILE_FOLDER, _GIT_FOLDER, _DB_FOLDER):
        create_directory_if_absent(path)
    setup_logger()
    with Database() as cursors:
        set_client_if_new(cursors.metadata)
        # Get client data early since cursors are invalid in different thread
        client_data = Client(**get_key_val(_CLIENT_KEY, cursors.metadata))
        Thread(target=lambda: setup_repository(client_data), name="SetupRepo").start()
        ensure_root_node(cursors)


def setup_logger() -> None:
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    file_handler = RotatingFileHandler(filename=_LOG_FILENAME, mode='w', maxBytes=512000, backupCount=4)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s [%(threadName)-12.12s] %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    for handler in (file_handler, StreamHandler()):
        logger.addHandler(handler)
    logger.critical("== STARTING on " + datetime.today().isoformat() + " ==")


def get_uuid() -> NodeId:
    return cast(NodeId, urlsafe_b64encode(uuid4().bytes).decode())
