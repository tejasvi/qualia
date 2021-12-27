from functools import lru_cache, cache, cached_property
from pathlib import Path
from typing import cast
from uuid import UUID

from orderedset import OrderedSet

from qualia.config import _SHORT_ID_STORE_BYTES
from qualia.models import MinimalDb, NodeId, NodeShortId, Li, KeyNotFoundError, FullId
from qualia.services.utils.git_utils import repository_file_to_content_children
from qualia.utils.common_utils import get_id_in_file_name, counter, short_id_encoder


class FileDb(MinimalDb):
    _buffer_id_external_node_id_map: dict[NodeShortId, NodeId] = {}

    def __init__(self, source_id):
        super().__init__()  # TODO
        self.source_id = source_id
        self.directory = NotImplemented

    @lru_cache(1000)
    def _get_node_content_descendants(self, node_id: NodeId) -> tuple[Li, OrderedSet[NodeId]]:
        encrypted = False  # TODO: Should support?
        content, children = repository_file_to_content_children(self.directory.joinpath(node_id + ".md"), encrypted)
        return content, children

    def get_node_descendants(self, node_id: NodeId, transposed: bool, discard_invalid: bool, temporary) -> OrderedSet[NodeId]:
        assert not transposed, "FileDb does not support fetching parents."
        return self._get_node_content_descendants(node_id)[1]

    def get_node_content_lines(self, node_id: NodeId, temporary) -> Li:
        return self._get_node_content_descendants(node_id)[0]

    @cached_property
    def get_root_id(self) -> NodeId:
        first_file = next((name for name in self.directory.iterdir() if name.is_file())).as_posix()
        file_id = get_id_in_file_name(first_file, ".md")
        try:
            UUID(file_id)
        except ValueError:
            raise Exception(f"NodeId parsed from {first_file}: {file_id} is not valid.")
        return cast(NodeId, file_id)