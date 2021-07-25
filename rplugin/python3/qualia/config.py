from pathlib import Path

from appdirs import user_data_dir

_app_folder_path = Path(user_data_dir("qualianotes", "qualia"))
try:
    _app_folder_path.mkdir(parents=True, exist_ok=True)
except FileExistsError as e:
    if not (_app_folder_path.is_symlink() and _app_folder_path.is_dir()):
        raise Exception(f"{_app_folder_path} already exists as a file.")

GIT_FOLDER: str = _app_folder_path.joinpath("git").as_posix()
DB_FOLDER: str = _app_folder_path.joinpath("db").as_posix()

CONFLICTS: str = "conflicts"
_git_token = "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"
GIT_URL = f"https://{_git_token}@github.com/tejasvi8874/qualia"
LEVEL_SPACES = 4
EXPANDED_BULLET = '-'
TO_EXPAND_BULLET = '*'
COLLAPSED_BULLET = '+'
