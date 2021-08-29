from __future__ import annotations

from typing import cast, Optional

from qualia.buffer import Process
from qualia.models import View, NodeId, Cursors, LastSeen, Tree
from qualia.services.realtime import Realtime
from qualia.services.utils.realtime_utils import sync_with_realtime_db
from qualia.utils.common_utils import get_key_val, sync_with_db


def sync_buffer(buffer_lines: list[str], main_id: NodeId, last_seen: LastSeen, cursors: Cursors,
                transposed: bool, realtime_session: Realtime) -> View:
    if main_id in last_seen:
        main_view, changes = Process().process_lines(buffer_lines, main_id, last_seen, cursors)
        realtime_data = sync_with_db(main_view, changes, last_seen, cursors, transposed, realtime_session.others_online)
        sync_with_realtime_db(realtime_data, realtime_session)
    else:
        main_view = View(main_id, cast(Optional[Tree],
                                       get_key_val(main_id, cursors.transposed_views if transposed else cursors.views,
                                                   False)) or {})
    return main_view
