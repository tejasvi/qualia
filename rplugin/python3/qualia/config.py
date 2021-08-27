from __future__ import annotations

from os import chmod, PathLike
from pathlib import Path
from shutil import rmtree
from stat import S_IWRITE
from typing import Callable

from appdirs import user_data_dir

DEBUG = True
NVIM_DEBUG_PIPE = r'\\.\pipe\nvim-15600-0'  # E.g. nvim --listen \\.\pipe\nvim-15600-0 test.md

QUALIA_DATA_DIR = user_data_dir("qualianotes", "qualia")
if DEBUG:
    QUALIA_DATA_DIR += '_debug'

FIREBASE_WEB_APP_CONFIG = {
    "apiKey": "AIzaSyDFNIazv7K0qDDJriiYPbhmB3OzUJYJvMI",
    "authDomain": "qualia-321013.firebaseapp.com",
    "databaseURL": "https://qualia-321013-default-rtdb.firebaseio.com",
    "projectId": "qualia-321013",
    "storageBucket": "qualia-321013.appspot.com",
    "messagingSenderId": "707949243379",
    "appId": "1:707949243379:web:db239176c6738dc5578086",
    "measurementId": "G-BPNP22GS5X"
}

GIT_TOKEN = 'ghp_QJSHBmXvDAbjiiI' 'BHTDEb3yryLofv52dcTbP'
GIT_BRANCH = "master"
GIT_REPOSITORY = "github.com/tejasvi8874/test"
GIT_TOKEN_URL = f"https://{GIT_TOKEN}@{GIT_REPOSITORY}"
GIT_SEARCH_URL = f"https://{GIT_REPOSITORY}/search?q="

NEST_LEVEL_SPACES = 4

# Internal constants

_APP_FOLDER_PATH = Path(QUALIA_DATA_DIR)

_FILE_FOLDER = _APP_FOLDER_PATH.joinpath("files")
_DB_FOLDER = _APP_FOLDER_PATH.joinpath("db")
_GIT_FOLDER = _APP_FOLDER_PATH.joinpath("git")
_LOG_FILENAME = _APP_FOLDER_PATH.joinpath('logs')

_RESET_APP_FOLDER = True
if _RESET_APP_FOLDER:
    def onerror(func: Callable[[PathLike], None], path: PathLike, exc_info) -> None:
        if exc_info[0] is FileNotFoundError:
            pass
        else:
            chmod(path, S_IWRITE)
            func(path)


    rmtree(_APP_FOLDER_PATH, onerror=onerror)

_EXPANDED_BULLET = '-'
_TO_EXPAND_BULLET = '*'
_COLLAPSED_BULLET = '+'
_CONTENT_CHILDREN_SEPARATOR_LINES = ["<hr>", ""]
_FZF_LINE_DELIMITER = "\t"

_ROOT_ID_KEY = "root_id"
_CLIENT_KEY = "client"

_GIT_FLAG_ARG = "git"
