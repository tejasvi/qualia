from multiprocessing.connection import Listener
from random import Random
from time import sleep
from typing import Iterator, Optional

from psutil import Process

from qualia.utils.common_utils import logger

_seeded_random = Random(923487)


def deterministic_random_ephemeral_ports(max_count: int) -> Iterator[int]:
    for _ in range(max_count):
        yield _seeded_random.randrange(49152, 65536)


def create_listener() -> Listener:
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
                logger.debug(f"Port {_LISTENER_PORT} already in use by {proc.name()} with PID={proc.pid}."
                             " Retrying after 30s")
                sleep(10)
    return listener


def find_port_process(port: int) -> Optional[Process]:
    from psutil import process_iter
    for proc in process_iter():
        for conns in proc.connections(kind='inet'):
            if conns.laddr.port == port:
                return proc
    return None
