from __future__ import annotations

from collections import UserDict
from dataclasses import dataclass
from subprocess import CalledProcessError
from typing import NewType, Union, Optional, Tuple, Dict, MutableMapping, List, Callable

from lmdb import Cursor
from orderedset import OrderedSet
from typing_extensions import TypedDict

from qualia.config import _ENCRYPTION_USED

NodeId = NewType("NodeId", str)
StringifiedChildren = NewType("StringifiedChildren", str)
StringifiedContent = NewType("StringifiedContent", str)
BufferId = Tuple[int, str]
BufferNodeId = NewType("BufferNodeId", str)
Tree = Dict[NodeId, Optional[dict]]
LineRange = Tuple[int, int]
AstMap = Tuple[int, int]
El = NewType("El", list[str])
Li = NewType("Li", list[str])
ListenerRequest = tuple[str, list, dict[str, object]]


class DbClient(TypedDict):
    client_id: str
    client_name: str


@dataclass
class View:
    main_id: NodeId
    sub_tree: Optional[Tree]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: Dict[NodeId, Li] = {}
        self.changed_descendants_map: Dict[NodeId, OrderedSet[NodeId]] = {}

    def __bool__(self) -> bool:
        return bool(self.changed_descendants_map or self.changed_content_map)

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


@dataclass
class UncertainNodeChildrenException(Exception):
    node_id: NodeId
    line_range: Tuple[int, int]


@dataclass
class LineInfo:
    node_id: NodeId
    parent_view: View
    nested_level: int


@dataclass
class DuplicateNodeException(Exception):
    node_id: NodeId
    line_ranges: Tuple[LineRange, LineRange]


@dataclass
class NodeData:
    content_lines: List[str]
    descendants_ids: OrderedSet[NodeId]


class CustomCalledProcessError(CalledProcessError):
    def __init__(self, exp: CalledProcessError):
        self.__dict__.update(exp.__dict__)

    def __str__(self) -> str:
        return super().__str__() + str(self.stderr) + str(self.stdout)


class GitMergeError(CustomCalledProcessError):
    pass


class LastSync(UserDict, MutableMapping[NodeId, NodeData]):
    def __init__(self) -> None:
        super().__init__()
        self.data: Dict[NodeId, NodeData] = {}
        self.line_info: Dict[int, LineInfo] = {}

    def __clear__(self) -> None:
        self.data.clear()
        self.line_info.clear()

    def pop_data(self, node_id: NodeId) -> None:
        self.data.pop(node_id)


JSONType = Union[str, int, float, bool, None, Dict[str, object], List[str], List[object], Li, El]
NODE_ID_ATTR = "node_id"


@dataclass
class Cursors:
    content: Cursor
    children: Cursor
    views: Cursor

    unsynced_content: Cursor
    unsynced_children: Cursor
    unsynced_views: Cursor

    buffer_id_bytes_node_id: Cursor
    node_id_buffer_id: Cursor

    metadata: Cursor

    bloom_filters: Cursor

    parents: Cursor
    transposed_views: Cursor


class RealtimeDbIndexDisabledError(Exception):
    def __init__(self, e) -> None:
        super().__init__(
            'Ensure {"rules": {"connections": {".indexOn": ".value"}}} in Realtime Database rules section\n' + str(e))


RealtimeChildren = dict[NodeId, tuple[str, List[NodeId]]]
RealtimeContent = dict[NodeId, tuple[str, Li]]
RealtimeData = Union[RealtimeChildren, RealtimeContent]

RealtimeStringifiedChildren = Dict[NodeId, Tuple[str, StringifiedChildren]]
RealtimeStringifiedContent = Dict[NodeId, Tuple[str, StringifiedContent]]
RealtimeStringifiedData = Union[RealtimeStringifiedChildren, RealtimeStringifiedContent]


class RealtimeBroadcastPacket(TypedDict, total=False):
    children: RealtimeChildren
    content: RealtimeContent
    client_id: str
    timestamp: int
    encryption_enabled: bool


BufferContentSetter = Callable[[int, Union[str, Li]], None]
GitChangedNodes = dict[NodeId, tuple[OrderedSet[NodeId], Li]]


class KeyNotFoundError(Exception):
    pass


class AbstractFernet:
    def __init__(self, _key: bytes):
        pass

    def _error(self) -> bytes:
        raise NotImplementedError(f"Encryption or decryption requested but {_ENCRYPTION_USED=}")

    def decrypt(self, _token: bytes) -> bytes:
        return self._error()

    def encrypt(self, _data: bytes) -> bytes:
        return self._error()
