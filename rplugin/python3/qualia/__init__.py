try:
    from qualia.commands import Qualia
except ModuleNotFoundError:
    from qualia.utils.requirements_utils import install_qualia_dependencies

    install_qualia_dependencies()
    from qualia.commands import Qualia
