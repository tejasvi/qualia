from os import environ, name, pathsep
from pathlib import Path
from subprocess import check_call, CalledProcessError, run
from sys import executable


def get_location(exe_name: str) -> str:
    return run(['where' if name == 'nt' else 'which', exe_name], capture_output=True, text=True).stdout.rstrip('\n')


def install_qualia_dependencies(optional_install_dir: str) -> None:
    try:
        check_call([executable, "-m", "ensurepip", "--default-pip"])
    except CalledProcessError:
        pass

    requirements_file_path = Path(__file__).parent.parent.parent.joinpath("requirements.txt").as_posix()

    init_command = [executable, "-m", "pip", "install", "setuptools", "wheel"]
    install_command = [executable, "-m", "pip", "install", "-r", requirements_file_path]

    try:
        for command in (init_command, install_command):
            check_call(command + ["--user"] )
    except CalledProcessError:
        try:
            for command in (init_command, install_command):
                check_call(command)
        except CalledProcessError:
            default_python = [get_location("py"), "-3"] if name == 'nt' else [get_location("python3")]
            try:
                check_call(default_python + ["-m", "ensurepip", "--default-pip"])
            except CalledProcessError:
                pass
            check_call(default_python + init_command[1:])
            check_call(default_python + install_command[1:], env=dict(environ, PIP_TARGET=optional_install_dir,
                                                                      PYTHONPATH=(environ["PYTHONPATH"] + pathsep +
                                                                                  optional_install_dir) if "PYTHONPATH" in environ else optional_install_dir))
