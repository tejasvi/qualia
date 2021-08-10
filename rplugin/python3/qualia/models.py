from __future__ import annotations

from collections import UserDict
from dataclasses import dataclass
from typing import NewType, Union, Any, Optional, Callable, Tuple, Dict, MutableMapping, List, FrozenSet

from lmdb import Cursor
from orderedset import OrderedSet
from typing_extensions import TypedDict

NodeId = NewType("NodeId", str)
BufferId = Tuple[int, str]
BufferNodeId = NewType("BufferNodeId", str)
Tree = Dict[NodeId, Optional[dict]]
LineRange = Tuple[int, int]


class Client(TypedDict):
    client_id: str
    client_name: str


@dataclass
class View:
    main_id: NodeId
    sub_tree: Union[Tree, None]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: Dict[NodeId, List[str]] = {}
        self.changed_children_map: Dict[NodeId, OrderedSet[NodeId]] = {}

    def __bool__(self) -> bool:
        return bool(self.changed_children_map or self.changed_content_map)

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


@dataclass
class UncertainNodeChildrenException(Exception):
    node_id: NodeId
    line_range: Tuple[int, int]


@dataclass
class LineInfo:
    node_id: NodeId
    context: Tree


@dataclass
class DuplicateNodeException(Exception):
    node_id: NodeId
    line_ranges: Tuple[LineRange, LineRange]


@dataclass
class NodeData:
    content_lines: List[str]
    children_ids: FrozenSet[NodeId]


class LastSeen(UserDict, MutableMapping[NodeId, NodeData]):
    def __init__(self) -> None:
        super().__init__()
        self.data: Dict[NodeId, NodeData] = {}
        self.line_info: Dict[int, LineInfo] = {}

    def __clear__(self) -> None:
        self.data.clear()
        self.line_info.clear()

    def clear_except_main(self, node_id: NodeId):
        self.data: Dict[NodeId, NodeData] = {node_id: self.data.pop(node_id)}
        self.line_info: Dict[int, LineInfo] = {0: LineInfo(node_id, {node_id: {}})}


JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
NODE_ID_ATTR = "node_id"


@dataclass
class Cursors:
    content: Cursor
    children: Cursor
    views: Cursor

    unsynced_content: Cursor
    unsynced_children: Cursor
    unsynced_views: Cursor

    buffer_to_node_id: Cursor
    node_to_buffer_id: Cursor

    metadata: Cursor

    bloom_filters: Cursor

    parents: Cursor
    inverted_views: Cursor


class NotNodeDirectory(Exception):
    """The directory is invalid node. Should contain README.md and name should be hex encoded UUID"""


ConflictHandlerData = Union[List[str], Union[List[str], List[NodeId]]]
ConflictHandler = Callable[[NodeId, ConflictHandlerData, Cursor], ConflictHandlerData]

RealtimeChildrenData = Dict[NodeId, Tuple[str, List[NodeId]]]
RealtimeContentData = Dict[NodeId, Tuple[str, List[str]]]


class RealtimeData(TypedDict, total=False):
    children: RealtimeChildrenData
    content: RealtimeContentData
    client_id: str
