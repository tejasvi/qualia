from __future__ import annotations

from threading import current_thread
from time import sleep
from typing import TYPE_CHECKING, Callable

from qualia.config import FIREBASE_WEB_APP_CONFIG
from qualia.models import RealtimeSync, RealtimeDbIndexDisabledError
from qualia.services.utils.common_utils import get_trigger_event
from qualia.services.utils.realtime_utils import process_children_broadcast, process_content_broadcast, CHILDREN_KEY, \
    CONTENT_KEY, RealtimeUtils, network_errors, tuplify_values
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import Database, logger, exception_traceback, StartLoggedThread

if TYPE_CHECKING:
    from firebase_admin.db import Event as FirebaseEvent


class Realtime(RealtimeUtils):
    def __init__(self, buffer_sync_trigger: Callable) -> None:
        super().__init__()
        logger.critical("Enter")
        # self.nvim = nvim
        self.unsynced_changes_event = get_trigger_event(buffer_sync_trigger, 0.1)
        StartLoggedThread(target=self.initialize, name="InitRealtime")

    def connect_firebase(self) -> None:
        import firebase_admin  # Takes ~0.8 s
        import firebase_admin.db as db

        logger.debug("Connecting firebase")
        default_app = firebase_admin.initialize_app(options=FIREBASE_WEB_APP_CONFIG)

        self.data_ref = db.reference('/data', default_app)
        self.data_ref.listen(self.broadcast_listener)

        self.connections_ref = db.reference('/connections', default_app)
        self.others_online = self.check_others_online()
        self.connections_ref.listen(self.new_client_listener)

        StartLoggedThread(target=self.update_online_status, name="UpdateOnlineStatus")

    def update_online_status(self) -> None:
        from requests import HTTPError
        logger.debug("In online update")
        try:
            while True:
                try:
                    cur_time_sec = self._accurate_seconds()
                    self.connections_ref.update({self.client_id: cur_time_sec})
                    sleep(1)
                except HTTPError as e:
                    raise RealtimeDbIndexDisabledError(e) if ".indexOn" in repr(e) else e
                except network_errors():
                    sleep(5)
        except Exception as e:
            logger.critical("Error while updating status " + exception_traceback(e))
            raise e

    def check_others_online(self) -> bool:
        from requests import HTTPError  # Takes ~0.1s
        others_online = False
        try:
            connected_clients, etag = self.connections_ref.get(True)
            if not connected_clients:
                return False
            offline_clients = []
            cur_time_sec = self._accurate_seconds()
            for client_id, timestamp in connected_clients.items():
                if timestamp > cur_time_sec - 5:
                    if client_id != self.client_id:
                        others_online = True
                else:
                    offline_clients.append(client_id)
            if offline_clients:
                for client_id in offline_clients:
                    connected_clients.pop(client_id)
                self.connections_ref.set_if_unchanged(etag, connected_clients)
        except HTTPError as e:
            raise RealtimeDbIndexDisabledError(e) if ".indexOn" in repr(e) else e
        except network_errors():
            pass
        return others_online

    def broadcast_listener(self, event):
        # type:(Realtime, FirebaseEvent) -> None
        current_thread().name = "BroadcastListener"
        value: RealtimeSync = event.data
        if not value or (value["client_id"] == self.client_id) or (value["timestamp"] < self._accurate_seconds() - 5):
            return
        logger.debug(f"Listener got a signal {value}")
        broadcast_conflicts: RealtimeSync = {}
        children_changed = content_changed = False
        with Database() as cursors:
            if CHILDREN_KEY in value:
                children_changed, children_conflicts = process_children_broadcast(tuplify_values(value[CHILDREN_KEY]),
                                                                                  cursors)
                if children_conflicts:
                    broadcast_conflicts[CHILDREN_KEY] = children_conflicts
            if CONTENT_KEY in value:
                content_changed, content_conflicts = process_content_broadcast(tuplify_values(value[CONTENT_KEY]),
                                                                               cursors)
                if content_conflicts:
                    broadcast_conflicts[CONTENT_KEY] = content_conflicts
        if broadcast_conflicts:
            self.client_broadcast(broadcast_conflicts)
        if children_changed or content_changed:
            self.unsynced_changes_event.set()


if __name__ == "__main__":
    bootstrap()
    while True:
        sleep(0.5)
