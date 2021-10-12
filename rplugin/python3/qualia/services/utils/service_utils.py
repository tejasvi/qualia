from threading import Event, current_thread
from time import sleep
from typing import Callable, Optional

from qualia.database import Database
from qualia.models import NodeId, KeyNotFoundError
from qualia.utils.common_utils import StartLoggedThread, live_logger, exception_traceback, ordered_data_hash


def get_task_firing_event(task: Callable, throttle_seconds: float) -> Event:
    service_event = Event()

    def event_watcher() -> None:
        from inspect import getsource
        try:
            current_thread().setName(getsource(task).split('lambda', 1)[-1].strip()
                                     if task.__name__ == '<lambda>' else task.__name__)
        except Exception as e:
            live_logger.critical("Could set name of task event thread" + exception_traceback(e))
        while True:
            service_event.wait()
            service_event.clear()
            task()
            sleep(throttle_seconds)

    StartLoggedThread(event_watcher, "EventWatcher", 0)
    return service_event


def content_hash(node_id: NodeId, db: Database) -> Optional[str]:
    try:
        return ordered_data_hash(db.get_node_content_lines(node_id))
    except KeyNotFoundError:
        return None
