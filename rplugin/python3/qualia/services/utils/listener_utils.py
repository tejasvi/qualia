from random import Random
from time import sleep
from typing import Iterator, Optional, TYPE_CHECKING

from qualia.utils.common_utils import live_logger

if TYPE_CHECKING:
    from psutil import Process
    from multiprocessing.connection import Listener

_seeded_random = Random(923487)


def deterministic_random_ephemeral_ports(max_count: int) -> Iterator[int]:
    for _ in range(max_count):
        yield _seeded_random.randrange(49152, 65536)


def create_listener():
    # type: () -> Listener
    from multiprocessing.connection import Listener
    _LISTENER_PORT = 1200
    while True:
        try:
            listener = Listener(('localhost', _LISTENER_PORT))
            break
        except OSError as e:
            proc = find_port_process(_LISTENER_PORT)
            if proc is None:
                raise e
            else:
                live_logger.debug(f"Port {_LISTENER_PORT} already in use by {proc.name()} with PID={proc.pid}."
                                  " Retrying after 30s")
                sleep(10)
    return listener


def find_port_process(port):
    # type: (int) -> Optional[Process]
    from psutil import process_iter
    for proc in process_iter():
        for conns in proc.connections(kind='inet'):
            if conns.laddr.port == port:
                return proc
    return None
