from pathlib import Path
from subprocess import check_call, CalledProcessError
from sys import executable


def install_qualia_dependencies() -> None:
    requirements_file_path = Path(__file__).parent.parent.parent.joinpath("requirements.txt").as_posix()
    install_command = [executable, "-m", "pip", "install", "-r", requirements_file_path]
    try:
        check_call(install_command)
    except CalledProcessError:
        check_call(install_command + ["--user"])
