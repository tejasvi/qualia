from __future__ import annotations

# from qualia.utils.perf_utils import perf_imports
#
# perf_imports()

# if True:
#     from typeguard.importhook import install_import_hook
#
#     install_import_hook('qualia')

from sys import path, version_info, argv

# Detect if loaded as plugin or from external script
if argv[-1].endswith("qualia") or argv[-1].endswith("__init__.py"):
    from pathlib import Path
    from importlib.util import find_spec
    from logging import getLogger
    from traceback import format_exception

    from qualia.config import _LOGGER_NAME
    from qualia.utils.init_utils import install_dependencies, setup_logger

    assert version_info[:2] >= (3, 7), "Use python version equal or higher than 3.7"

    "http://ix.io/3xdL"
    "https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type"

    _logger = getLogger(_LOGGER_NAME)

    setup_logger(_logger)

    optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
    path.append(optional_install_dir)

    try:

        from qualia.plugin import Qualia

        for package in ('firebase_admin', 'markdown_it', 'pynvim'):  # Lazy loaded
            if not find_spec(package):
                raise ModuleNotFoundError
    except ModuleNotFoundError as e:
        _logger.critical("Certain packages are missing " + str(e) + "Attempting installation")
        install_dependencies(optional_install_dir, _logger)
        from qualia.plugin import Qualia

        _logger.critical("Certain packages were missing and are now installed. Run :UpdateRemotePlugins again")
    except BaseException as e:
        _logger.critical('\n'.join(format_exception(None, e, e.__traceback__)))
        raise e

"""
https://cryptography.io/en/latest/fernet/#implementation
"""
