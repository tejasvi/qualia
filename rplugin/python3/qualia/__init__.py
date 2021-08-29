from __future__ import annotations

from logging import getLogger
from pathlib import Path
from sys import path, version_info
from traceback import format_exception

from qualia.config import _LOGGER_NAME
from qualia.utils.init_utils import install_dependencies, setup_logger

assert version_info[:2] >= (3, 7), "Use python version equal or higher than 3.7"

"http://ix.io/3xdL"

_logger = getLogger(_LOGGER_NAME)

setup_logger(_logger)

optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
path.append(optional_install_dir)
try:
    from qualia.plugin import Qualia
except ModuleNotFoundError as e:
    _logger.critical("Certain packages are missing " + str(e) + "Attempting installation")
    install_dependencies(optional_install_dir)
    _logger.critical("Certain packages were missing and are now installed. Run :UpdateRemotePlugins again")
except Exception as e:
    _logger.critical('\n'.join(format_exception(None, e, e.__traceback__)))
    raise e
