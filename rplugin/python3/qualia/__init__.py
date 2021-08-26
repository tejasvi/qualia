from qualia.qualia import Qualia
from qualia.utils.perf_utils import start_time

try:
    from qualia.qualia import Qualia
except ModuleNotFoundError:
    from qualia.utils.requirements_utils import install_qualia_dependencies

    install_qualia_dependencies()
    from qualia.qualia import Qualia
