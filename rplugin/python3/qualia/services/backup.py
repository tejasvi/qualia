from datetime import datetime
from glob import glob
from pathlib import Path
from threading import Timer
from zipfile import ZipFile, ZIP_DEFLATED

from qualia.config import _DB_FOLDER, _BACKUP_COUNT, _BACKUP_DAYS_INTERVAL


def backup_db() -> None:
    now = datetime.today()
    backup_file_suffix = ".data.mdb.zip"
    backup_files = sorted(glob(_DB_FOLDER.as_posix() + "/*-*-*T*;*;*.*" + backup_file_suffix))

    if backup_files:
        last_backup_file = backup_files[-1]
        last_backup_time = datetime.fromisoformat(
            removesuffix(Path(last_backup_file).name, backup_file_suffix).replace(";", ":"))
        seconds_since_backup = (now - last_backup_time).total_seconds()
        backup_seconds_interval = _BACKUP_DAYS_INTERVAL * 24 * 60 * 60
        if seconds_since_backup < backup_seconds_interval:
            Timer(backup_seconds_interval - seconds_since_backup, backup_db).start()
            return

    for _ in range(len(backup_files) - _BACKUP_COUNT):
        stale_backup_file = backup_files.pop()
        Path(stale_backup_file).unlink()
    with ZipFile(_DB_FOLDER.joinpath(now.isoformat().replace(":", ";") + backup_file_suffix), compresslevel=9,
                 compression=ZIP_DEFLATED, mode='x') as backup_zip:
        backup_zip.write(_DB_FOLDER.joinpath('data.mdb'))


def removesuffix(input_string: str, suffix: str) -> str:
    # in 3.9 str.removesuffix
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string
