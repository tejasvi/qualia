from collections import UserDict
from dataclasses import dataclass
from typing import NewType, Union, Any, TypedDict, Optional, Callable

from lmdb import Cursor
from orderedset import OrderedSet

NodeId = NewType("NodeId", str)
BufferId = tuple[int, str]
BufferNodeId = NewType("BufferNodeId", str)
Tree = dict[NodeId, Optional[dict]]
LineRange = tuple[int, int]


class Client(TypedDict):
    client_id: str
    client_name: str


@dataclass
class View:
    main_id: NodeId
    sub_tree: Union[Tree, None]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: dict[NodeId, list[str]] = {}
        self.changed_children_map: dict[NodeId, OrderedSet[NodeId]] = {}

    def __bool__(self) -> bool:
        return bool(self.changed_children_map or self.changed_content_map)

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


@dataclass
class UncertainNodeChildrenException(Exception):
    node_id: NodeId
    line_range: tuple[int, int]


@dataclass
class LineInfo:
    node_id: NodeId
    context: Tree


@dataclass
class DuplicateNodeException(Exception):
    node_id: NodeId
    line_ranges: tuple[LineRange, LineRange]


@dataclass
class NodeData:
    content_lines: list[str]
    children_ids: frozenset[NodeId]


class LastSeen(UserDict[NodeId, NodeData]):
    def __init__(self) -> None:
        super().__init__()
        self.data: dict[NodeId, NodeData] = {}
        self.line_info: dict[int, LineInfo] = {}

    def __clear__(self) -> None:
        self.data.clear()
        self.line_info.clear()

    def clear_except_main(self, node_id: NodeId):
        self.data: dict[NodeId, NodeData] = {node_id: self.data.pop(node_id)}
        self.line_info: dict[int, LineInfo] = {0: LineInfo(node_id, {node_id: {}})}


JSONType = Union[str, int, float, bool, None, dict[str, Any], list[Any]]
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


ConflictHandlerData = Union[list[str], Union[list[str], list[NodeId]]]
ConflictHandler = Callable[[NodeId, ConflictHandlerData, Cursor], ConflictHandlerData]

RealtimeChildrenData = dict[NodeId, tuple[str, list[NodeId]]]
RealtimeContentData = dict[NodeId, tuple[str, list[str]]]


class RealtimeData(TypedDict, total=False):
    children: RealtimeChildrenData
    content: RealtimeContentData
    client_id: str
