from __future__ import annotations

from sys import argv
from typing import cast

from qualia.config import DEBUG, ATTACH_PYCHARM
from qualia.config import _ENCRYPTION_USED

# from qualia.utils.perf_utils import perf_imports
#
# perf_imports()
# from typeguard.importhook import install_import_hook
#
# install_import_hook('qualia')

if DEBUG and ATTACH_PYCHARM:
    # https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#remote-debug-config
    try:
        import pydevd_pycharm

        pydevd_pycharm.settrace('localhost', port=9001, stdoutToServer=True, stderrToServer=True, suspend=False)
        from time import time

        print(f"Starting at {time()}s")
    except ConnectionRefusedError:
        pass


def _get_plugin_class() -> object:
    from importlib.util import find_spec

    assert version_info[:2] >= (3, 7), "Use python version equal or higher than 3.7"

    from qualia.plugin import Qualia

    for package in ('firebase_admin', 'markdown_it', 'pynvim', 'cryptography'):  # Lazy loaded
        if not find_spec(package):
            raise ModuleNotFoundError

    return cast(object, Qualia)


def accelerated_import() -> None:
    # TODO: Profile the improvement
    if _ENCRYPTION_USED:
        def _fernet_importer() -> None:
            import cryptography.fernet  # noqa

        from threading import Thread
        Thread(target=_fernet_importer, name="FernetImporter").start()


# Detect if loaded as plugin or from external script
if argv[-1].endswith("qualia") or argv[-1].endswith("__init__.py"):
    accelerated_import()

    from pathlib import Path
    from sys import path, version_info

    from traceback import format_exception
    from qualia.utils.init_utils import install_dependencies, setup_logger
    from qualia.config import _LOGGER_NAME

    optional_install_dir = Path().home().joinpath('.qualia_packages').as_posix()
    path.append(optional_install_dir)

    _logger = setup_logger()

    try:
        QualiaPlugin = _get_plugin_class()
    except ModuleNotFoundError as e:
        _logger.critical("Certain packages are missing " + str(e) + "Attempting installation")

        install_dependencies(optional_install_dir, _logger)

        QualiaPlugin = _get_plugin_class()

        _logger.critical("Certain packages were missing and are now installed. Run :UpdateRemotePlugins again")
    except BaseException as e:
        _logger.critical('\n'.join(format_exception(None, e, e.__traceback__)))
        raise

"""
http://ix.io/3xdL"
https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type"
https://cryptography.io/en/latest/fernet/#implementation
"""
