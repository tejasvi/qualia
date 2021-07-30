from pathlib import Path
from shutil import rmtree

from appdirs import user_data_dir

APP_FOLDER_PATH = Path(user_data_dir("qualianotes", "qualia"))

FILE_FOLDER = APP_FOLDER_PATH.joinpath("files")

DB_FOLDER = APP_FOLDER_PATH.joinpath("db")
rmtree(DB_FOLDER, ignore_errors=True)

GIT_FOLDER = APP_FOLDER_PATH.joinpath("git")
rmtree(GIT_FOLDER, ignore_errors=True)

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
