from _sha256 import sha256
from base64 import urlsafe_b64encode
from json import dumps
from threading import Thread
from typing import Union, Iterable

from lmdb import Cursor
from orderedset import OrderedSet

from qualia.models import JSONType, RealtimeData, NodeId
from qualia.utils.common_utils import get_key_val, conflict


def value_hash(key: str, cursor: Cursor) -> str:
    data_bytes = cursor.get(key.encode())
    return realtime_data_hash(data_bytes)


def realtime_data_hash(data: Union[bytes, JSONType]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def sync_with_realtime_db(data: RealtimeData, realtime_session) -> None:
    if data and realtime_session.others_online:
        def broadcast_closure() -> None:
            realtime_session.client_broadcast(data)

        Thread(target=broadcast_closure, name="ClientBroadcast").start()


def merge_children_with_local(node_id: NodeId, new_children_ids: Iterable[NodeId], children_cur: Cursor) -> list[
    NodeId]:
    merged_children_ids = OrderedSet(get_key_val(node_id, children_cur))
    merged_children_ids.update(new_children_ids)
    return list(merged_children_ids)


def merge_content_with_local(node_id: NodeId, new_content_lines: list[str], content_cur: Cursor) -> list[str]:
    db_content_lines: list[str] = get_key_val(node_id, content_cur)
    return conflict(new_content_lines, db_content_lines)
