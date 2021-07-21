from dataclasses import dataclass
from typing import NewType, Union, Any

NodeId = NewType("NodeId", str)
BufferNodeId = NewType("BufferNodeId", str)


@dataclass
class View:
    root_id: NodeId
    sub_tree: dict[str, dict]


class ProcessState:
    def __init__(self) -> None:
        self.changed_content_map: dict[NodeId, list[str]] = {}
        self.changed_children_map: dict[NodeId, set[str]] = {}

    def __bool__(self) -> bool:
        return bool(self.changed_children_map or self.changed_content_map)

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


@dataclass
class DuplicateException(Exception):
    node_id: NodeId
    loc_1: tuple[int, int]
    loc_2: tuple[int, int]


class CloneException(DuplicateException):
    pass


@dataclass
class NodeData:
    content_lines: list[str]
    children_ids: frozenset[NodeId]
    buffer_id: Union[None, BufferNodeId]


class LedgerBase(dict):
    # Created during each buffer writing event
    def __init__(self, **kwargs: NodeData):
        super().__init__(**kwargs)
        # Not cleared on self.clear()
        self.buffer_node_id_map: dict[BufferNodeId, NodeId] = {}


Ledger = LedgerBase[NodeId, NodeData]

JSONType = Union[str, int, float, bool, None, dict[str, Any], list[Any]]
NODE_ID_ATTR = "node_id"
