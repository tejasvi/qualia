import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from os import environ, name, pathsep
from pathlib import Path
from subprocess import run
from sys import executable
from typing import Optional


def get_location(exe_name: str) -> str:
    return run(['where' if name == 'nt' else 'which', exe_name], capture_output=True, text=True).stdout.rstrip('\n')


class ProcessException(Exception):
    pass


def install_dependencies(optional_install_dir: str, logger: Optional[logging.Logger]) -> None:
    def cmd(*args, **kwargs) -> None:
        res = run(*args, **kwargs, capture_output=True)
        if res.returncode:
            raise ProcessException(res.returncode, (str(args) + str(kwargs))[20:], res.stdout, res.stderr)
        if logger:
            logger.critical(f"Running {args} and {kwargs}\n{res.stdout}\n{res.stderr}")

    try:
        cmd([executable, "-m", "ensurepip", "--default-pip"])
    except ProcessException:
        pass

    requirements_file_path = Path(__file__).parent.parent.joinpath("requirements.txt").as_posix()

    init_command = [executable, "-m", "pip", "install", "setuptools", "wheel"]
    install_command = [executable, "-m", "pip", "install", "-r", requirements_file_path]

    try:
        for command in (init_command, install_command):
            cmd(command + ["--user"])
    except ProcessException:
        try:
            for command in (init_command, install_command):
                cmd(command)
        except ProcessException:
            default_python = [get_location("py"), "-3"] if name == 'nt' else [get_location("python3")]
            try:
                cmd(default_python + ["-m", "ensurepip", "--default-pip"])
            except ProcessException:
                pass
            env = dict(environ, PIP_TARGET=optional_install_dir,
                       PYTHONPATH=(environ["PYTHONPATH"] + pathsep + optional_install_dir
                                   ) if "PYTHONPATH" in environ else optional_install_dir)
            try:
                for command in (init_command, install_command):
                    cmd(default_python + command[1:], env=env)
            except ProcessException:
                for pkg in (["setuptools", "wheel"], ["-r", requirements_file_path]):
                    cmd([get_location("pip"), "install"] + pkg, env=env)


_logger_setup: bool = False


def setup_logger() -> logging.Logger:
    assert not _logger_setup, "Logger already setup"
    from qualia.config import _LOG_FILENAME, _LOGGER_NAME

    # Singleton logger
    logger = logging.getLogger(_LOGGER_NAME)

    logger.setLevel(logging.DEBUG if logging.DEBUG else logging.INFO)
    file_handler = RotatingFileHandler(filename=_LOG_FILENAME, mode='w', maxBytes=512000, backupCount=4)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s [%(threadName)-12.12s] %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.critical("== STARTING on " + datetime.today().isoformat() + " ==")
    logger.addHandler(logging.StreamHandler())

    return logger
