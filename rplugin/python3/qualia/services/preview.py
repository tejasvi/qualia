from time import time

s = time()
from pathlib import Path
from sys import path
from typing import TYPE_CHECKING

path.append(Path(__file__).parent.parent.parent.as_posix())  # noqa: E402
from qualia.services.utils.preview_utils import InvalidParameters, parse_args, connect_listener

if TYPE_CHECKING:
    from qualia.models import ListenerRequest


def main() -> None:
    try:
        attach_running_process, node_id_arg, output_width, output_height = parse_args()
    except InvalidParameters:
        return

    if attach_running_process:  # Takes under 0.1s if uneeded imports are lazy loaded
        conn = connect_listener()
        if conn:
            preview_node_request: ListenerRequest = ('preview_node', [node_id_arg, output_width, output_height, 3], {})
            conn.send(preview_node_request)
            recieved_data = conn.recv()
            print('\n'.join(recieved_data) if isinstance(recieved_data, list) else recieved_data)
            close_connection_request: ListenerRequest = ('close_connection', [], {})
            conn.send(close_connection_request)
            return

    # Import only if no listener running (used as external tool)

    from qualia.services.utils.preview_utils import preview_node
    from typing import cast
    from qualia.utils.common_utils import exception_traceback
    from qualia.models import NodeId
    from qualia.config import _PREVIEW_NEST_LEVEL

    preview_node_id = cast(NodeId, node_id_arg)
    try:
        print(
            '\n'.join(cast(list[str], preview_node(preview_node_id, output_width, output_height, _PREVIEW_NEST_LEVEL))))
    except Exception as e:
        print("Some error occured" + exception_traceback(e))


if __name__ == '__main__':
    main()
