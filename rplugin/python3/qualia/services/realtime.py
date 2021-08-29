from __future__ import annotations

from threading import Event
from time import sleep, time
from typing import Final
from typing import TYPE_CHECKING

# from firebasedata import LiveData, FirebaseData
from ntplib import NTPClient
from pynvim import Nvim

from qualia.config import FIREBASE_WEB_APP_CONFIG
from qualia.models import RealtimeData
from qualia.services.utils.realtime_utils import process_children_broadcast, process_content_broadcast
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import Database, logger, exception_traceback, StartLoggedThread, \
    get_set_client

# for deferred_import in ("pyrebase", "requests"):
#     if find_spec(deferred_import) is None:
#         raise ModuleNotFoundError(f"No module named '{deferred_import}'")

if TYPE_CHECKING:
    from pyrebase.pyrebase import Pyre
    from pyrebase import pyrebase

CHILDREN_KEY: Final = "children"
CONTENT_KEY: Final = "content"


class Realtime:
    db: pyrebase.Database
    # live_data: LiveData
    client_id: str
    offset_seconds: float

    def __init__(self, nvim: Nvim) -> None:
        logger.critical("Enter")
        self.nvim = nvim
        self.others_online: bool = False
        self.initialization_event = Event()
        StartLoggedThread(target=self.initialize, name="InitRealtime")

    def initialize(self) -> None:
        logger.debug("Initializing")
        with Database() as cursors:
            self.client_id = get_set_client(cursors.metadata)["client_id"]
        while True:
            try:
                self.offset_seconds = NTPClient().request('pool.ntp.org').offset
                self.connect_firebase()
            except ConnectionError as e:
                logger.critical("Couldn't connect to firebase\n" + exception_traceback(e))
                sleep(5)
            except Exception as e:
                logger.critical("Firebase error\n" + exception_traceback(e))
                break
            else:
                self.initialization_event.set()
                break

    def connect_firebase(self) -> None:
        logger.debug("Connecting firebase")
        try:
            from pyrebase import pyrebase  # Blocks thread for a while
        except (ModuleNotFoundError, ImportError) as e:
            logger.critical("Pyrebase not installed")
            raise e
        app = pyrebase.initialize_app(FIREBASE_WEB_APP_CONFIG)
        # self.live_data = live_data = LiveData(app, '/')
        # live_data.listen()
        self.db = app.database()

        # live_data.signal('/data').connect(self._broadcast_listener)
        self.db.child("data").stream(self._broadcast_listener)
        # live_data.signal('/connections').connect(self._new_client_listener)
        self.db.child('connections').stream(self._new_client_listener)
        logger.debug("Before online")
        logger.debug("Online update")
        StartLoggedThread(target=self._update_online_status, name="UpdateOnline")

    def _accurate_seconds(self) -> int:
        return int(self.offset_seconds + time())

    def _update_online_status(self) -> None:
        logger.debug("In online update")
        from requests import HTTPError, ConnectionError  # Takes ~0.1s
        try:
            while True:
                cur_time_sec = self._accurate_seconds()
                try:
                    connected_clients: list[Pyre] = self.db.child('/connections').order_by_value().start_at(
                        cur_time_sec - 5).get().pyres
                except HTTPError as e:
                    raise Exception(
                        'Ensure {"rules": {"connections": {".indexOn": ".value"}}} in Realtime Database rules section\n' +
                        repr(e)) if ".indexOn" in repr(e) else e
                except ConnectionError:
                    sleep(5)
                else:
                    for client_id, _timestamp in [pyre.item for pyre in connected_clients]:
                        if client_id != self.client_id:
                            self.others_online = True
                            break
                    else:
                        self.others_online = False
                    # self.live_data.set_data(f'/connections/{self.client_id}', cur_time_sec)
                    self.db.child('connections').child(self.client_id).set(cur_time_sec)
                    sleep(1)
        except Exception as e:
            logger.critical("Error while updating status " + exception_traceback(e))
            raise e

    # def _broadcast_listener(self, _sender: FirebaseData, value: RealtimeData, **_path) -> None:
    def _broadcast_listener(self, message) -> None:
        print(message)
        value = message["data"]
        logger.debug("Got signal")
        logger.debug(value["client_id"])
        if value["client_id"] == self.client_id:
            return
        logger.debug(f"Listener got a signal {id(value)} {list(value.keys())}")
        conflicts: RealtimeData = {}
        with Database() as cursors:
            if CHILDREN_KEY in value:
                children_conflicts = process_children_broadcast(value[CHILDREN_KEY], cursors)
            if CONTENT_KEY in value:
                content_conflicts = process_content_broadcast(value[CONTENT_KEY], cursors)
        if children_conflicts:
            conflicts[CHILDREN_KEY] = children_conflicts
        if content_conflicts:
            conflicts[CONTENT_KEY] = content_conflicts
        if conflicts:
            self.client_broadcast(conflicts)
        logger.debug("Before call")
        StartLoggedThread(lambda: self.nvim.async_call(self.nvim.command, "normal vyvp"), "Trigger call")
        logger.debug("After call")

    def client_broadcast(self, data: RealtimeData):
        if (data.get(CHILDREN_KEY) or data.get(CONTENT_KEY)) and self.initialization_event.wait(5):
            logger.debug(data)
            data["client_id"] = self.client_id
            # self.live_data.set_data('/data', data)
            self.db.child('data').set(data)

    # def _new_client_listener(self, _sender: FirebaseData, value: dict[str, int], **_path) -> None:
    def _new_client_listener(self, message) -> None:
        value = message['data']
        logger.debug("Client listener", str(value))
        if value:
            new_client_id, _ = value.popitem()
            if self.client_id != new_client_id:
                self.others_online = True


if __name__ == "__main__":
    bootstrap()
    while True:
        sleep(0.5)
