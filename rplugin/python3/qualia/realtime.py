from threading import Thread, Event
from time import sleep, time
from typing import Optional

import pyrebase
from firebasedata import LiveData, FirebaseData
from ntplib import NTPClient
from pyrebase import pyrebase
from pyrebase.pyrebase import Pyre
from requests import HTTPError, ConnectionError

from qualia.config import FIREBASE_WEB_APP_CONFIG
from qualia.models import ConflictHandlerData, ConflictHandler, RealtimeData, NodeId
from qualia.utils import Database, get_key_val, put_key_val, bootstrap
from qualia.utils import merge_children_with_local, merge_content_with_local, value_hash, realtime_data_hash

CHILDREN_KEY = "children"
CONTENT_KEY = "content"


class Realtime:
    def __init__(self) -> None:
        self.others_online: bool = False
        self.db: Optional[pyrebase.Database] = None
        self.live: Optional[LiveData] = None
        self.client_id: Optional[str] = None
        self.offset_seconds: Optional[float] = None
        self.initialization_event = Event()
        Thread(target=self.initialize).start()

    def initialize(self) -> None:
        while True:
            try:
                app = pyrebase.initialize_app(FIREBASE_WEB_APP_CONFIG)
                self.db = app.database()
                self.live = live = LiveData(app, '/')
                live.listen()
                live.signal('/data').connect(self._broadcast_handler)
                with Database() as cursors:
                    self.client_id = get_key_val("client", cursors.metadata)["client_id"]
                self.offset_seconds = NTPClient().request('pool.ntp.org').offset
                Thread(target=self._update_online_status).start()
                live.signal('/connections').connect(self._new_client_listener)
                self.initialization_event.set()
                break
            except ConnectionError as e:
                print("Couldn't connect to firebase\n", e)
                sleep(5)
            except Exception as e:
                print("Firebase error\n", e)
                break

    def _accurate_seconds(self) -> int:
        return int(self.offset_seconds + time())

    def _update_online_status(self) -> None:
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
                self.live.set_data(f'/connections/{self.client_id}', cur_time_sec)
                sleep(1)

    def _broadcast_handler(self, _sender: FirebaseData, value: RealtimeData, **_path) -> None:
        print(value)
        with Database() as cursors:
            if value["client_id"] == self.client_id:
                return
            print("signal", value)
            conflicts: RealtimeData = {}
            if CHILDREN_KEY in value:
                conflicts[CHILDREN_KEY] = Realtime._process_broadcast_data(value[CHILDREN_KEY], cursors.children,
                                                                           merge_children_with_local)
            if CONTENT_KEY in value:
                conflicts[CONTENT_KEY] = Realtime._process_broadcast_data(value[CONTENT_KEY], cursors.content,
                                                                          merge_content_with_local)
            self.client_broadcast(conflicts)

    @staticmethod
    def _process_broadcast_data(data_dict: dict[NodeId, tuple[str, ConflictHandlerData]], cursor,
                                conflict_handler: ConflictHandler) -> \
            dict[NodeId, tuple[str, ConflictHandlerData]]:
        conflicts: dict[NodeId, tuple[str, ConflictHandlerData]] = {}
        for node_id, (last_hash, data) in data_dict.items():
            db_hash = value_hash(node_id, cursor)
            if db_hash != last_hash:
                data = conflict_handler(node_id, data, cursor)
                new_hash = realtime_data_hash(data)
                conflicts[node_id] = new_hash, data
            put_key_val(node_id, data, cursor, True)
        return conflicts

    def client_broadcast(self, data: RealtimeData):
        if data and self.initialization_event.wait(5):
            data["client_id"] = self.client_id
            self.live.set_data('/data', data)

    def _new_client_listener(self, _sender: FirebaseData, value: dict[str, int], **_path) -> None:
        if value:
            new_client_id, _ = value.popitem()
            if self.client_id != new_client_id:
                self.others_online = True


if __name__ == "__main__":
    bootstrap()
    while True:
        sleep(0.5)
