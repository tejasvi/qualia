from os import environ, name, pathsep
from pathlib import Path
from subprocess import run
from sys import executable


def get_location(exe_name: str) -> str:
    return run(['where' if name == 'nt' else 'which', exe_name], capture_output=True, text=True).stdout.rstrip('\n')


class ProcessException(Exception):
    pass


def cmd(*args, **kwargs) -> None:
    res = run(*args, **kwargs)
    if res.returncode:
        raise ProcessException(res.returncode, str(args) + str(kwargs), res.stdout, res.stderr)


def install_qualia_dependencies(optional_install_dir: str) -> None:
    try:
        cmd([executable, "-m", "ensurepip", "--default-pip"])
    except ProcessException:
        pass

    requirements_file_path = Path(__file__).parent.parent.parent.joinpath("requirements.txt").as_posix()

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
            for command in (init_command, install_command):
                cmd(default_python + command[1:], env=env)
