from os import environ
from pathlib import Path
from subprocess import check_call, CalledProcessError
from sys import executable


def install_qualia_dependencies(optional_install_dir: str) -> None:
    check_call([executable, " -m", "ensurepip", "--default-pip"])
    requirements_file_path = Path(__file__).parent.parent.parent.joinpath("requirements.txt").as_posix()
    install_command = [executable, "-m", "pip", "install", "-r", requirements_file_path]
    try:
        check_call(install_command + ["--user"])
    except CalledProcessError:
        try:
            check_call(install_command)
        except CalledProcessError:
            check_call(install_command, env=dict(environ, PIP_TARGET=optional_install_dir))
