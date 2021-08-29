from __future__ import annotations

from collections import UserDict
from dataclasses import dataclass
from subprocess import CalledProcessError
from typing import NewType, Union, Any, Optional, Tuple, Dict, MutableMapping, List, Callable

from lmdb import Cursor
from orderedset import OrderedSet
from typing_extensions import TypedDict

NodeId = NewType("NodeId", str)
BufferId = Tuple[int, str]
BufferNodeId = NewType("BufferNodeId", str)
Tree = Dict[NodeId, Optional[dict]]
LineRange = Tuple[int, int]
AstMap = Tuple[int, int]


class Client(TypedDict):
    client_id: str
    client_name: str


@dataclass
class View:
    main_id: NodeId
    sub_tree: Optional[Tree]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: Dict[NodeId, List[str]] = {}
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
    context: Tree


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
        return super().__str__() + self.stderr + self.stdout


class LastSeen(UserDict, MutableMapping[NodeId, NodeData]):
    def __init__(self) -> None:
        super().__init__()
        self.data: Dict[NodeId, NodeData] = {}
        self.line_info: Dict[int, LineInfo] = {}

    def __clear__(self) -> None:
        self.data.clear()
        self.line_info.clear()

    def pop_data(self, node_id: NodeId) -> None:
        self.data.pop(node_id)


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
    transposed_views: Cursor


ChildrenConflictData = dict[NodeId, tuple[str, List[NodeId]]]
ContentConflictData = dict[NodeId, tuple[str, List[str]]]
ConflictData = Union[ChildrenConflictData, ContentConflictData]

RealtimeChildrenData = Dict[NodeId, Tuple[str, List[NodeId]]]
RealtimeContentData = Dict[NodeId, Tuple[str, List[Union[str, NodeId]]]]


class RealtimeData(TypedDict, total=False):
    children: RealtimeChildrenData
    content: RealtimeContentData
    client_id: str


BufferContentSetter = Callable[[int, Union[str, list[str]]], None]