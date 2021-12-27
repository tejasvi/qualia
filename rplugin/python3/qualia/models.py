from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from subprocess import CalledProcessError
from threading import Event
from typing import NewType, Union, Optional, Tuple, Dict, MutableMapping, List, Callable, Container, Iterable, TYPE_CHECKING

from lmdb import Cursor
from orderedset import OrderedSet
from typing_extensions import TypedDict

from qualia.config import _ENCRYPTION_USED, _GIT_FOLDER, _SHORT_ID, _DB_FOLDER

if TYPE_CHECKING:
    from qualia.database import MaDatabase

StringifiedChildren = NewType("StringifiedChildren", str)
StringifiedContent = NewType("StringifiedContent", str)

BufferId = Tuple[int, str]

NodeId = NewType("NodeId", str)
NodeShortId = NewType("NodeShortId", str)

SourceId = NewType("SourceId", str)
SourceShortId = NewType("SourceShortId", str)

ShortId = Union[NodeShortId, SourceShortId]
FullId = Union[NodeId, SourceId]

Tree = dict[NodeId, Optional[dict]]
LineRange = Tuple[int, int]
AstMap = Tuple[int, int]
El = NewType("El", list[str])
Li = NewType("Li", list[str])
ListenerRequest = tuple[str, list, dict[str, object]]


@dataclass
class View:
    main_id: NodeId
    source_id: SourceId
    sub_tree: Optional[Tree]  # None should indicate unknown
    transposed: bool


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
    def __init__(self, source_id: SourceId) -> None:
        super().__init__()
        self.data: Dict[NodeId, NodeData] = {}
        self.line_info: Dict[int, LineInfo] = {}
        self.source_id = source_id

    def __clear__(self) -> None:
        self.data.clear()
        self.line_info.clear()
        self.source_id = None

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

    temp_content: Cursor
    temp_children: Cursor
    temp_parents: Cursor

    imported_node_id_source_id: Cursor

    bloom_filters: Cursor

    parents: Cursor
    transposed_views: Cursor

    metadata: Cursor


@dataclass
class QCursors:
    short_id_bytes_node_id: Cursor
    node_id_short_id: Cursor

    short_id_bytes_source_id: Cursor
    source_id_short_id: Cursor

    source_id_info: Cursor

    metadata: Cursor


class MinimalDb(ABC):
    """For supporting alternative data sources in future (e.g. git repo)"""

    def __init__(self, _source_location: str, _password_callback: Callable[[], str])->None:
        self.main_db: Optional[MaDatabase] = None

    def __enter__(self) -> MinimalDb:
        pass

    def __exit__(self, *_) -> None:
        pass

    @abstractmethod
    def get_node_descendants(self, node_id: NodeId, transposed: bool, discard_invalid: bool, temporary) -> OrderedSet[NodeId]:
        """If parent info is not available in O(1), return empty data instead. Parent data is not assumed to be reliable
        :param temporary:
        """
        pass

    @abstractmethod
    def get_node_content_lines(self, node_id: NodeId, temporary) -> Li:
        pass

    @abstractmethod
    def db_encrypted(self) -> bool:
        pass

    @abstractmethod
    def get_root_id(self) -> NodeId:
        pass

    @abstractmethod
    def set_root_id(self, root_id: NodeId) -> None:
        pass

    @abstractmethod
    def set_source_id(self, source_id: SourceId, main_db: MaDatabase) -> None:
        pass

    @abstractmethod
    def get_set_source_id(self, main_db: MaDatabase) -> SourceId:
        pass

    @abstractmethod
    def get_set_source_name(self) -> str:
        pass

    @abstractmethod
    def set_source_name(self, source_name: str) -> None:
        pass

    @abstractmethod
    def get_node_ids(self, temporary) -> list[NodeId]:
        pass

    @abstractmethod
    def get_set_keywords(self, node_id) -> Container:
        pass

    @abstractmethod
    def is_valid_node(self, node_id: NodeId) -> bool:
        pass


class MutableDb(MinimalDb, ABC):
    @abstractmethod
    def bootstrap(self) -> None:
        pass

    @abstractmethod
    def set_node_descendants(self, node_id: NodeId, descendant_ids: OrderedSet[NodeId], transposed: bool):
        pass

    @abstractmethod
    def set_node_view(self, view: View) -> None:
        pass

    @abstractmethod
    def delete_node(self, node_id: NodeId) -> None:
        pass

    @abstractmethod
    def set_node_content_lines(self, node_id: NodeId, content_lines: Li, ) -> None:
        pass

    @abstractmethod
    def children_hash(self, node_id: NodeId) -> str:
        pass

    @abstractmethod
    def get_node_view(self, node_id: NodeId, transposed: bool, main_db: "MaDatabase") -> View:
        pass


class SyncableDb(MutableDb, ABC):
    def __init__(self, _source_location: str, _password_callback: Callable[[], str]) -> None:
        super().__init__()
        self.git_repository_data_subpath: Path = Path("data")
        self.repository_setup = Event()
        self.bootstrap()

    def git_repository_dir(self, main_db: MaDatabase)->Path:
        source_id = self.get_set_source_id()
        return _GIT_FOLDER.joinpath(main_db.full_to_short_id(source_id, False) if _SHORT_ID else source_id)

    def git_repository_data_dir(self, main_db: MaDatabase)->Path:
        return self.git_repository_dir(main_db).joinpath("data")

    @abstractmethod
    def delete_unsynced_content_children(self, node_id: NodeId) -> None:
        pass

    @abstractmethod
    def if_unsynced_children(self, node_id: NodeId) -> bool:
        pass

    @abstractmethod
    def if_unsynced_content(self, node_id: NodeId) -> bool:
        pass

    @abstractmethod
    def pop_unsynced_node_ids(self) -> Iterable[NodeId]:
        pass

    @abstractmethod
    def needs_first_use_password(self) -> bool:
        pass


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


class InvalidNodeId:
    pass


class InvalidFileChildrenLine(Exception):
    pass


class DbType(Enum):
    LMDB = "lmdb"
