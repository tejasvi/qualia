from pathlib import Path

from appdirs import user_data_dir, user_config_dir

from qualia.utils.config_utils import create_directory_if_absent, force_remove_directory

FIREBASE_WEB_APP_CONFIG = {
    # On https://console.firebase.google.com (free plan),
    # Go to Project Settings -> Add app -> "</>" (web app option)
    # Set name -> Continue -> Use the displayed "firebaseConfig"
    "apiKey": "AIzaSyDFNIazv7K0qDDJriiYPbhmB3OzUJYJvMI",
    "authDomain": "qualia-321013.firebaseapp.com",
    "databaseURL": "https://qualia-321013-default-rtdb.firebaseio.com",
    "projectId": "qualia-321013",
    "storageBucket": "qualia-321013.appspot.com",
    "messagingSenderId": "707949243379",
    "appId": "1:707949243379:web:db239176c6738dc5578086",
    "measurementId": "G-BPNP22GS5X"
}

_GIT_TOKEN = 'ghp_QJSHBmXvDAbjiiI' 'BHTDEb3yryLofv52dcTbP'
_GIT_REPOSITORY = "github.com/tejasvi8874/test"
GIT_TOKEN_URL = f"https://{_GIT_TOKEN}@{_GIT_REPOSITORY}"
GIT_SEARCH_URL = f"https://{_GIT_REPOSITORY}/search?q="
GIT_BRANCH = "master"

NEST_LEVEL_SPACES = 4

DEBUG = True
NVIM_DEBUG_PIPE = r'\\.\pipe\nvim-15600-0'  # E.g. nvim --listen \\.\pipe\nvim-15600-0 test.md

QUALIA_DATA_DIR = user_data_dir("qualianotes", "qualia")
if DEBUG:
    QUALIA_DATA_DIR += '_debug'

# Internal constants

_APP_FOLDER_PATH = Path(QUALIA_DATA_DIR)
_RESET_APP_FOLDER = False
if _RESET_APP_FOLDER:
    force_remove_directory(_APP_FOLDER_PATH)

_FILE_FOLDER = _APP_FOLDER_PATH.joinpath("files")
_DB_FOLDER = _APP_FOLDER_PATH.joinpath("db")
_GIT_FOLDER = _APP_FOLDER_PATH.joinpath("git")
_LOG_FILENAME = _APP_FOLDER_PATH.joinpath('logs')

for path in (_APP_FOLDER_PATH, _FILE_FOLDER, _GIT_FOLDER, _DB_FOLDER):
    create_directory_if_absent(path)

_SHORT_BUFFER_ID = True

_EXPANDED_BULLET = '-'
_TO_EXPAND_BULLET = '*'
_COLLAPSED_BULLET = '+'
_CONTENT_CHILDREN_SEPARATOR_LINES = ["<hr>", ""]
_FZF_LINE_DELIMITER = "\t"

_ROOT_ID_KEY = "root_id"
_CLIENT_KEY = "client"

_GIT_FLAG_ARG = "git"
_LOGGER_NAME = "qualia"
