from abc import ABCMeta, abstractmethod
from json import loads, JSONDecodeError, dumps
from socket import gaierror
from threading import Event, current_thread
from time import time, sleep
from typing import Iterable, TYPE_CHECKING, cast, Literal, Union, Iterator

from ntplib import NTPClient, NTPException
from orderedset import OrderedSet

from qualia.config import ENCRYPT_REALTIME, FIREBASE_WEB_APP_CONFIG
from qualia.database import Database
from qualia.models import RealtimeBroadcastPacket, NodeId, RealtimeContent, RealtimeStringifiedData, \
    RealtimeStringifiedContent, RealtimeStringifiedChildren, StringifiedChildren, \
    StringifiedContent, El, Li, RealtimeChildren
from qualia.services.utils.service_utils import content_hash
from qualia.utils.common_utils import conflict, live_logger, \
    ordered_data_hash, StartLoggedThread, exception_traceback, decrypt_lines, encrypt_lines, \
    children_data_hash, absent_node_content_lines

if TYPE_CHECKING:
    from firebase_admin.db import Reference, Event as FirebaseEvent
    from firebase_admin import App
    from requests import ConnectionError  # Takes ~0.1s
    from urllib3.exceptions import MaxRetryError
else:
    # Dummy type class
    class Reference:
        pass


def sync_with_realtime_db(data: RealtimeBroadcastPacket, realtime_session) -> None:
    if data and realtime_session.others_online:
        def broadcast_closure() -> None:
            realtime_session.client_broadcast(data)

        StartLoggedThread(target=broadcast_closure, name="ClientBroadcast", delay_seconds=0)


def merge_children_with_local(node_id: NodeId, new_children_ids: Iterable[NodeId], db: Database) -> OrderedSet[
    NodeId]:
    merged_children_ids = db.get_node_descendants(node_id, False, False)
    merged_children_ids.update(new_children_ids)
    return OrderedSet(sorted(merged_children_ids))  # To prevent cyclic conflicts)


def merge_content_with_local(node_id: NodeId, new_content_lines: Li, db: Database) -> Li:
    db_content_lines = db.get_node_content_lines(node_id)
    return new_content_lines if db_content_lines is None else conflict(new_content_lines, db_content_lines)


def process_content_broadcast(data_dict: RealtimeStringifiedContent, db: Database, content_encrypted: bool) -> \
        tuple[
            bool, RealtimeContent]:
    content_conflicts: RealtimeContent = {}
    data_changed = False

    for downstream_data, last_hash, node_id in parse_realtime_data_item(data_dict):
        downstream_content = decrypt_lines(cast(El, downstream_data)) if content_encrypted else cast(Li,
                                                                                                     downstream_data)
        downstream_hash = ordered_data_hash(downstream_content)
        db_hash = content_hash(node_id, db)

        if downstream_hash == db_hash:
            continue  # spurious rebroadcasts
        data_changed = True

        previous_version_mismatch = db_hash is not None and db_hash != last_hash
        if previous_version_mismatch:
            downstream_content = merge_content_with_local(node_id, downstream_content, db)
            content_conflicts[node_id] = downstream_hash, downstream_content
        db.set_node_content_lines(node_id, downstream_content)

    return data_changed, content_conflicts


def process_children_broadcast(data_dict: RealtimeStringifiedChildren, db: Database) -> tuple[
    bool, RealtimeBroadcastPacket]:
    broadcast_conflicts: RealtimeBroadcastPacket = {}
    data_changed = False

    for downstream_data, last_hash, node_id in parse_realtime_data_item(data_dict):
        downstream_children_ids = OrderedSet(cast(list[NodeId], downstream_data))
        downstream_hash = children_data_hash(downstream_children_ids)
        db_hash = db.children_hash(node_id)
        if downstream_hash == db_hash:
            continue  # spurious rebroadcasts
        data_changed = True
        previous_version_mismatch = db_hash != last_hash
        if previous_version_mismatch:
            merged_children = merge_children_with_local(node_id, downstream_children_ids, db)
            downstream_missing_children_ids = merged_children.difference(downstream_children_ids)

            override_hash = downstream_hash
            if downstream_missing_children_ids:
                # Send content else if new child content unknown to reciever cyclic:  invalid > ignored > rebroadcast
                broadcast_conflicts.setdefault(CONTENT_KEY, cast(RealtimeContent, {})).update(
                    {child_id: (ordered_data_hash(
                        absent_node_content_lines), db.get_node_content_lines(child_id)) for child_id
                        in downstream_missing_children_ids})
                override_hash = db_hash

            broadcast_conflicts.setdefault(CHILDREN_KEY, cast(RealtimeChildren, {}))[node_id] = override_hash, list(
                merged_children)

            downstream_children_ids = merged_children
        db.set_node_descendants(node_id, downstream_children_ids, False)

    return data_changed, broadcast_conflicts


def parse_realtime_data_item(data_dict: RealtimeStringifiedData) -> Iterator[
    tuple[Union[list[NodeId], El], str, NodeId]]:
    for item in data_dict.items():
        try:
            node_id, (last_hash, stringified_downstream_data) = item
            downstream_data: Union[list[NodeId], El] = loads(stringified_downstream_data)
        except (ValueError, JSONDecodeError):
            live_logger.error(f"[Realtime Sync] Got corrupt value: {item}")
            continue
        yield downstream_data, last_hash, node_id


CHILDREN_KEY: Literal["children"] = "children"
CONTENT_KEY: Literal["content"] = "content"


class RealtimeUtils(metaclass=ABCMeta):
    data_ref: Reference
    connections_ref: Reference
    offset_seconds: float

    def __init__(self) -> None:
        with Database() as db:
            self.client_id = db.get_set_client()["client_id"]
        self.initialization_event = Event()
        self.others_online: bool = False

    def client_broadcast(self, broadcast_data: RealtimeBroadcastPacket) -> None:
        broadcast_needed = stringify_broadcast_data(broadcast_data)
        if broadcast_needed and self.initialization_event.wait(5):
            live_logger.debug(broadcast_data)
            broadcast_data["client_id"] = self.client_id
            broadcast_data["timestamp"] = self._accurate_seconds()
            broadcast_data["encryption_enabled"] = ENCRYPT_REALTIME
            try:
                self.data_ref.set(broadcast_data)
            except network_errors():
                live_logger.debug("Couldn't broadcast due to network error.")

    def new_client_listener(self, event):
        # type:(RealtimeUtils, FirebaseEvent) -> None
        current_thread().name = "ClientListener"
        value = event.data
        if value:
            new_client_id, _ = value.popitem()
            if self.client_id != new_client_id:
                self.others_online = True

    def _accurate_seconds(self) -> int:
        return int(self.offset_seconds + time())

    def initialize(self) -> None:
        live_logger.debug("Initializing")
        import firebase_admin  # Takes ~0.8 s

        live_logger.debug("Connecting firebase")
        default_app = firebase_admin.initialize_app(options=FIREBASE_WEB_APP_CONFIG)
        while True:
            try:
                self.offset_seconds = NTPClient().request('time.google.com').offset
                self.connect_firebase(default_app)
            except network_errors() as e:
                live_logger.debug("Couldn't connect to firebase\n" + exception_traceback(e))
                sleep(5)
            except Exception as e:
                live_logger.error("Firebase error\n" + exception_traceback(e))
                raise e
            else:
                self.initialization_event.set()
                break

    @abstractmethod
    def connect_firebase(self, app):
        # type: (App) -> None
        pass


def stringify_broadcast_data(broadcast_data: RealtimeBroadcastPacket) -> bool:
    # Firebase omits raw empty arrays, maps
    broadcast_needed = False
    for data_type_key, key_data in broadcast_data.items():
        if data_type_key == CHILDREN_KEY:
            key_data = cast(RealtimeChildren, key_data)
            for node_id, (last_hash, children) in key_data.items():
                key_data = cast(RealtimeStringifiedChildren, key_data)
                key_data[node_id] = last_hash, cast(StringifiedChildren, dumps(children))
                broadcast_needed = True
        elif data_type_key == CONTENT_KEY:
            key_data = cast(RealtimeContent, key_data)
            for node_id, (last_hash, content_lines) in key_data.items():
                key_data = cast(RealtimeStringifiedContent, key_data)
                key_data[node_id] = last_hash, cast(StringifiedContent, dumps(encrypt_lines(cast(Li, content_lines))
                                                                              if ENCRYPT_REALTIME else content_lines))
                broadcast_needed = True
    return broadcast_needed


def network_errors() -> tuple:
    from requests import ConnectionError
    from urllib3.exceptions import MaxRetryError
    from firebase_admin.exceptions import UnavailableError
    from google.auth.exceptions import TransportError
    return ConnectionError, gaierror, MaxRetryError, NTPException, UnavailableError, TransportError


def tuplify_values(dictionary: dict) -> RealtimeStringifiedData:
    """Keep mypy happy"""
    for k, v in dictionary.items():
        dictionary[k] = tuple(v)
    return dictionary
