from os import PathLike, chmod
from pathlib import Path
from shutil import rmtree
from stat import S_IWRITE
from typing import Callable, Union


def create_directory_if_absent(directory_path: Path) -> None:
    try:
        directory_path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (directory_path.is_symlink() and directory_path.is_dir()):
            raise Exception(f"\nMove {directory_path} file to some other location.\n\n")


def force_remove_directory(app_folder_path: PathLike) -> None:
    def onerror(func: Callable, path: Union[PathLike, str], exc_info: tuple) -> None:
        if exc_info[0] is FileNotFoundError:
            pass
        else:
            chmod(path, S_IWRITE)
            func(path)

    rmtree(app_folder_path, onerror=onerror)
