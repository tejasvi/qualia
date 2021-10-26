from __future__ import annotations

from queue import Queue, Empty
from sys import argv
from threading import current_thread
from time import sleep, time
from typing import TYPE_CHECKING, Callable, cast


from qualia.database import Database
from qualia.models import RealtimeBroadcastPacket, RealtimeDbIndexDisabledError, RealtimeStringifiedChildren, \
    RealtimeStringifiedContent, RealtimeContent, NodeId, Li
from qualia.services.utils.realtime_utils import process_children_broadcast, process_content_broadcast, CHILDREN_KEY, \
    CONTENT_KEY, RealtimeUtils, network_errors, tuplify_values
from qualia.services.utils.service_utils import get_task_firing_event
from qualia.utils.bootstrap_utils import bootstrap
from qualia.utils.common_utils import live_logger, exception_traceback, StartLoggedThread

if TYPE_CHECKING:
    from firebase_admin.db import Event as FirebaseEvent
    from firebase_admin import App


class Realtime(RealtimeUtils):
    def __init__(self, buffer_sync_trigger: Callable) -> None:
        super().__init__()
        live_logger.debug("Enter")

        self.broadcast_conflicts_queue: Queue[RealtimeBroadcastPacket] = Queue()

        self.last_broadcast_recieve_time = float('-inf')
        self.unsynced_changes_event = get_task_firing_event(buffer_sync_trigger, 0)
        StartLoggedThread(target=self.initialize, name="InitRealtime", delay_seconds=2)
        StartLoggedThread(target=self.watch_send_bulk_broadcast_conflicts, name="ConflictWatcher", delay_seconds=2)

    def connect_firebase(self, app):
        # type: (App) -> None
        import firebase_admin.db as db

        self.data_ref = db.reference('/data', app)
        self.data_ref.listen(self.broadcast_listener)

        self.connections_ref = db.reference('/connections', app)
        self.others_online = self.check_others_online()
        self.connections_ref.listen(self.new_client_listener)

        StartLoggedThread(target=self.update_online_status, name="UpdateOnlineStatus", delay_seconds=2)

    def update_online_status(self) -> None:
        from requests import HTTPError
        live_logger.debug("In online update")
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
            live_logger.critical("Error while updating online status " + exception_traceback(e))

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
        from cryptography.fernet import InvalidToken
        current_thread().name = "BroadcastListener"
        value: RealtimeBroadcastPacket = event.data
        for key in ("client_id", "timestamp"):
            if key not in value:
                live_logger.error(f"Value missing {key}; {value=}")
        if not value or (value.get("client_id") == self.client_id) or (
                value.get("timestamp", float('-inf')) < (self._accurate_seconds() - 5)):
            return
        live_logger.debug(f"Listener got a signal {value}")

        children_changed = content_changed = False
        broadcast_conflicts: RealtimeBroadcastPacket = {}

        with Database() as db:
            # Process content before to avoid discarding new children
            content_conflicts = None
            if CONTENT_KEY in value:
                try:
                    content_changed, content_conflicts = process_content_broadcast(
                        cast(RealtimeStringifiedContent, tuplify_values(value[CONTENT_KEY])),
                        db, 'encryption_enabled' in value and value['encryption_enabled'])
                except InvalidToken as e:
                    live_logger.critical(
                        "Can't decrypt broadcast content. Ensure the encryption keys match." + exception_traceback(e))
                    return
            if CHILDREN_KEY in value:
                children_downstream_data = cast(RealtimeStringifiedChildren, tuplify_values(value[CHILDREN_KEY]))
                children_changed, children_broadcast_conflicts = process_children_broadcast(children_downstream_data,
                                                                                            db)

                if content_conflicts:
                    children_broadcast_conflicts.setdefault(CONTENT_KEY, cast(RealtimeContent, {})).update(
                        content_conflicts)
                broadcast_conflicts = children_broadcast_conflicts
            else:
                if content_conflicts:
                    broadcast_conflicts = {CONTENT_KEY: content_conflicts}

        cur_time = time()
        if broadcast_conflicts:
            self.broadcast_conflicts_queue.put(broadcast_conflicts)
        if children_changed or content_changed:
            self.unsynced_changes_event.set()
            self.last_broadcast_recieve_time = cur_time

    def watch_send_bulk_broadcast_conflicts(self) -> None:
        last_conflict_time = float('-inf')
        wait_duration = 2
        while True:
            conflict = self.broadcast_conflicts_queue.get()

            if (accumulation_time_left := last_conflict_time - (time() - wait_duration)) > 0:
                # Accumulate if broadcasted conflicts recently
                sleep(accumulation_time_left)

            self.merge_newer_conflicts(conflict)
            self.client_broadcast(conflict)

            last_conflict_time = time()

    def merge_newer_conflicts(self, first_conflict: RealtimeBroadcastPacket) -> None:
        while True:
            try:
                next_conflict = self.broadcast_conflicts_queue.get_nowait()
            except Empty:
                break
            if CONTENT_KEY in next_conflict:
                first_conflict.setdefault(CONTENT_KEY, cast(dict[NodeId, tuple[str, Li]], {})).update(
                    next_conflict[CONTENT_KEY])
            if CHILDREN_KEY in next_conflict:
                first_conflict.setdefault(CHILDREN_KEY, cast(dict[NodeId, tuple[str, list[NodeId]]], {})).update(
                    next_conflict[CHILDREN_KEY])


if __name__ == "__main__" and argv[-1].endswith("realtime.py"):
    bootstrap()
    Realtime(lambda: None)
    live_logger.debug("Realtime sync started externally")
    while True:
        sleep(100)
