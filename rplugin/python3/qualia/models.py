from dataclasses import dataclass
from typing import NewType, Union, Any

import lmdb
from orderedset import OrderedSet

NodeId = NewType("NodeId", str)
BufferNodeId = NewType("BufferNodeId", str)
Tree = dict[NodeId, Union[dict, bool]]


@dataclass
class View:
    root_id: NodeId
    sub_tree: Union[Tree, None]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: dict[NodeId, list[str]] = {}
        self.changed_children_map: dict[NodeId, OrderedSet[str]] = {}

    def __bool__(self) -> bool:
        return bool(self.changed_children_map or self.changed_content_map)

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


@dataclass
class CloneChildrenException(Exception):
    node_id: NodeId
    loc: tuple[int, int]


@dataclass
class DuplicateException(Exception):
    node_id: NodeId
    loc_1: tuple[int, int]
    loc_2: tuple[int, int]


@dataclass
class NodeData:
    content_lines: list[str]
    children_ids: frozenset[NodeId]


Ledger = dict[NodeId, NodeData]

JSONType = Union[str, int, float, bool, None, dict[str, Any], list[Any]]
NODE_ID_ATTR = "node_id"


@dataclass
class Cursors:
    content: lmdb.Cursor
    children: lmdb.Cursor
    views: lmdb.Cursor

    unsynced_content: lmdb.Cursor
    unsynced_children: lmdb.Cursor
    unsynced_views: lmdb.Cursor

    buffer_to_node_id: lmdb.Cursor
    node_to_buffer_id: lmdb.Cursor
