from typing import Iterable, Optional

from lmdb import Cursor

from qualia.models import NodeId
from qualia.utils.common_utils import get_key_val, put_key_val


def add_remove_parent(add_or_remove: bool, parent_id: NodeId, children_ids: Iterable[NodeId], parents_cur: Cursor,
                      unsynced_children_cur: Optional[Cursor]):
    for children_id in children_ids:
        parent_id_list: list[str] = get_key_val(children_id, parents_cur) or []
        if add_or_remove:
            parent_id_list.append(parent_id)
        else:
            parent_id_list.remove(parent_id)
        put_key_val(children_id, parent_id_list, parents_cur, True)

        if unsynced_children_cur:
            put_key_val(children_id, True, unsynced_children_cur, True)
