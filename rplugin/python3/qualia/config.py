from __future__ import annotations
from os import chmod
from pathlib import Path
from shutil import rmtree
from stat import S_IWRITE

from appdirs import user_data_dir

DEBUG = True

data_dir = user_data_dir("qualianotes", "qualia")
if DEBUG:
    data_dir += '_debug'

APP_FOLDER_PATH = Path(data_dir)

if DEBUG:
    def onerror(func, path, exc_info):
        if exc_info[0] is FileNotFoundError:
            pass
        else:
            chmod(path, S_IWRITE)
            func(path)
    rmtree(APP_FOLDER_PATH, onerror=onerror)

FILE_FOLDER = APP_FOLDER_PATH.joinpath("files")

DB_FOLDER = APP_FOLDER_PATH.joinpath("db")

GIT_FOLDER = APP_FOLDER_PATH.joinpath("git")

GIT_TOKEN = 'ghp_QJSHBmXvDAbjiiI' 'BHTDEb3yryLofv52dcTbP'
GIT_TOKEN_URL = f"https://{GIT_TOKEN}@github.com/tejasvi8874/test"
GIT_URL = "https://github.com/tejasvi8874/test"
GIT_BRANCH = "master"
GIT_SEARCH_URL = f"{GIT_URL}/search?q="

CONFLICTS: str = "conflicts"
LEVEL_SPACES = 4
EXPANDED_BULLET = '-'
TO_EXPAND_BULLET = '*'
COLLAPSED_BULLET = '+'
ROOT_ID_KEY = "root_id"
CLIENT_KEY = "client"
LOG_FILENAME = APP_FOLDER_PATH.joinpath('logs')
CONTENT_CHILDREN_SEPARATOR_LINES = ["<hr>", ""]
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
