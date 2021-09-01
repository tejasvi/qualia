from abc import ABCMeta, abstractmethod
from json import loads, JSONDecodeError, dumps
from socket import gaierror
from threading import Event, current_thread
from time import time, sleep
from typing import Iterable, Optional, TYPE_CHECKING, cast, Literal, Type

from lmdb import Cursor
from ntplib import NTPClient, NTPException
from orderedset import OrderedSet

from qualia.config import DEBUG
from qualia.models import RealtimeSync, NodeId, Cursors, RealtimeSyncData, RealtimeSyncChildren, \
    RealtimeSyncContent, RealtimeData, RealtimeContent, RealtimeChildren, Stringified
from qualia.utils.common_utils import conflict, set_node_content_lines, logger, \
    realtime_data_hash, set_ancestor_descendants, StartLoggedThread, get_node_descendants, get_node_content, Database, \
    get_set_client, exception_traceback

if TYPE_CHECKING or DEBUG:
    from firebase_admin.db import Reference, Event as FirebaseEvent
    from requests import ConnectionError  # Takes ~0.1s
    from urllib3.exceptions import MaxRetryError
else:
    # Dummy type class
    class Reference:
        pass


    class FirebaseEvent:
        pass


def value_hash(key: str, cursor: Cursor) -> Optional[str]:
    data_bytes = cursor.get(key.encode())
    return None if data_bytes is None else realtime_data_hash(data_bytes)


def sync_with_realtime_db(data: RealtimeSync, realtime_session) -> None:
    if data and realtime_session.others_online:
        def broadcast_closure() -> None:
            realtime_session.client_broadcast(data)

        StartLoggedThread(target=broadcast_closure, name="ClientBroadcast")


def merge_children_with_local(node_id: NodeId, new_children_ids: Iterable[NodeId], cursors: Cursors) -> list[
    NodeId]:
    merged_children_ids = get_node_descendants(cursors, node_id, False, False)
    merged_children_ids.update(new_children_ids)
    return list(merged_children_ids)


def merge_content_with_local(node_id: NodeId, new_content_lines: list[str], cursors: Cursors) -> list[str]:
    db_content_lines: list[str] = get_node_content(cursors, node_id)
    return new_content_lines if db_content_lines is None else conflict(new_content_lines, db_content_lines)


def _process_broadcast_data(data_dict: RealtimeData, cursors: Cursors, is_content_or_children: bool) -> tuple[
    bool, RealtimeSyncData]:
    if TYPE_CHECKING:
        data_dict = cast(RealtimeSyncContent if is_content_or_children else RealtimeSyncChildren, data_dict)
    downstream_data_t = list[str] if is_content_or_children else list[NodeId]

    conflicts = cast(RealtimeSyncData, {})
    data_changed = False
    for item in data_dict.items():
        try:
            node_id, (last_hash, stringified_downstream_data) = item
            downstream_data: downstream_data_t = loads(stringified_downstream_data)
        except (ValueError, JSONDecodeError):
            logger.critical("[Realtime Sync] Got corrupt value: ", item)
        else:
            db_hash: Optional[str] = value_hash(node_id,
                                                cursors.content if is_content_or_children else cursors.children)
            downstream_hash = realtime_data_hash(downstream_data)
            if downstream_hash != db_hash:  # Check spurious rebroadcasts
                if db_hash is not None and db_hash != last_hash:
                    new_data: downstream_data_t = merge_content_with_local(
                        node_id, downstream_data, cursors
                    ) if is_content_or_children else merge_children_with_local(
                        node_id, downstream_data, cursors)
                    conflicts[node_id] = downstream_hash, new_data
                    downstream_data = new_data
                if is_content_or_children:
                    set_node_content_lines(downstream_data, cursors, node_id)
                else:
                    set_ancestor_descendants(cursors, OrderedSet(downstream_data), node_id, False)
                data_changed = True
    return data_changed, conflicts


def process_children_broadcast(data_dict: RealtimeChildren, cursors: Cursors) -> tuple[bool, RealtimeSyncChildren]:
    return cast(tuple[bool, RealtimeSyncChildren], _process_broadcast_data(data_dict, cursors, False))


def process_content_broadcast(data_dict: RealtimeContent, cursors: Cursors) -> tuple[bool, RealtimeSyncContent]:
    return cast(tuple[bool, RealtimeSyncContent], _process_broadcast_data(data_dict, cursors, True))


CHILDREN_KEY: Literal["children"] = "children"
CONTENT_KEY: Literal["content"] = "content"


class RealtimeUtils(metaclass=ABCMeta):
    data_ref: Reference
    connections_ref: Reference
    offset_seconds: float

    def __init__(self) -> None:
        with Database() as cursors:
            self.client_id = get_set_client(cursors.metadata)["client_id"]
        self.initialization_event = Event()
        self.others_online: bool = False

    def client_broadcast(self, broadcast_data: RealtimeSync) -> None:
        broadcast_needed = False
        for data_type_key, key_data in broadcast_data.items():
            if data_type_key in (CHILDREN_KEY, CONTENT_KEY):
                key_data = cast(RealtimeData, key_data)
                for node_id, (last_hash, node_data) in key_data.items():
                    key_data[node_id] = last_hash, cast(Stringified, dumps(node_data))  # Firebase drops empty arrays
                    broadcast_needed = True

        if broadcast_needed and self.initialization_event.wait(5):
            logger.debug(broadcast_data)
            broadcast_data["client_id"] = self.client_id
            broadcast_data["timestamp"] = self._accurate_seconds()
            try:
                self.data_ref.set(broadcast_data)
            except network_errors():
                logger.debug("Couldn't broadcast due to network error.")

    def new_client_listener(self, event: FirebaseEvent) -> None:
        current_thread().name = "ClientListener"
        value = event.data
        if value:
            new_client_id, _ = value.popitem()
            if self.client_id != new_client_id:
                self.others_online = True

    def _accurate_seconds(self) -> int:
        return int(self.offset_seconds + time())

    def initialize(self) -> None:
        logger.debug("Initializing")
        while True:
            try:
                self.offset_seconds = NTPClient().request('time.google.com').offset
                self.connect_firebase()
            except network_errors() as e:
                logger.critical("Couldn't connect to firebase\n" + exception_traceback(e))
                sleep(5)
            except Exception as e:
                logger.critical("Firebase error\n" + exception_traceback(e))
                raise e
            else:
                self.initialization_event.set()
                break

    @abstractmethod
    def connect_firebase(self) -> None:
        pass


def network_errors() -> tuple[Type[ConnectionError], Type[gaierror], Type[MaxRetryError], Type[NTPException]]:
    from requests import ConnectionError
    from urllib3.exceptions import MaxRetryError
    return ConnectionError, gaierror, MaxRetryError, NTPException


def tuplify_values(dictionary: dict) -> RealtimeData:
    # For mypy
    for k, v in dictionary.items():
        dictionary[k] = tuple(v)
    return dictionary
