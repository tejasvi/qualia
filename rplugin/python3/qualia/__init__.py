from pathlib import Path
from sys import path

optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
path.insert(0, optional_install_dir)
try:
    from qualia.commands import Qualia
except ModuleNotFoundError:
    from qualia.utils.requirements_utils import install_qualia_dependencies

    install_qualia_dependencies(optional_install_dir)
    from qualia.commands import Qualia
