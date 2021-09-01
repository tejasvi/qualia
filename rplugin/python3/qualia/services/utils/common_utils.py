from threading import Event, current_thread
from time import sleep
from typing import Callable

from qualia.utils.common_utils import StartLoggedThread, logger, exception_traceback


def get_trigger_event(callback: Callable, throttle_seconds: float) -> Event:
    service_event = Event()

    def event_watcher() -> None:
        from inspect import getsource
        try:
            current_thread().setName(getsource(callback).split('lambda', 1)[-1].strip()
                                     if callback.__name__ == '<lambda>' else callback.__name__)
        except Exception as e:
            logger.debug("Could set name of trigger event thread" + exception_traceback(e))
        while True:
            service_event.wait()
            service_event.clear()
            callback()
            sleep(throttle_seconds)

    StartLoggedThread(event_watcher, "")
    return service_event
