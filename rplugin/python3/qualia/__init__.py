from pathlib import Path
from sys import path

optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
path.append(optional_install_dir)
try:
    from qualia.commands import Qualia
except (ModuleNotFoundError, ImportError):
    from qualia.utils.requirements_utils import install_qualia_dependencies

    install_qualia_dependencies(optional_install_dir)
    raise Exception("Certain packages were missing and are now installed. Retrying :UpdateRemotePlugins")
