from __future__ import annotations

from logging import getLogger
from pathlib import Path
from sys import path, version_info, argv


# from qualia.utils.perf_utils import perf_imports
#
# perf_imports()
# if True:
#     from typeguard.importhook import install_import_hook
#
#     install_import_hook('qualia')


def main():
    # type: () -> "Qualia"
    from importlib.util import find_spec
    from qualia.config import DEBUG

    if DEBUG:
        # https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#remote-debug-config
        import pydevd_pycharm
        pydevd_pycharm.settrace('localhost', port=9001, stdoutToServer=True, stderrToServer=True)

    assert version_info[:2] >= (3, 7), "Use python version equal or higher than 3.7"

    from qualia.plugin import Qualia

    for package in ('firebase_admin', 'markdown_it', 'pynvim'):  # Lazy loaded
        if not find_spec(package):
            raise ModuleNotFoundError

    return Qualia


# Detect if loaded as plugin or from external script
if argv[-1].endswith("qualia") or argv[-1].endswith("__init__.py"):
    from traceback import format_exception
    from qualia.utils.init_utils import install_dependencies, setup_logger
    from qualia.config import _LOGGER_NAME

    setup_logger()
    _logger = getLogger(_LOGGER_NAME)

    try:
        QualiaPlugin = main()
    except ModuleNotFoundError as e:
        _logger.critical("Certain packages are missing " + str(e) + "Attempting installation")

        optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
        path.append(optional_install_dir)
        install_dependencies(optional_install_dir, _logger)

        QualiaPlugin = main()

        _logger.critical("Certain packages were missing and are now installed. Run :UpdateRemotePlugins again")
    except BaseException as e:
        _logger.critical('\n'.join(format_exception(None, e, e.__traceback__)))
        raise

"""
http://ix.io/3xdL"
https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type"
https://cryptography.io/en/latest/fernet/#implementation
"""
