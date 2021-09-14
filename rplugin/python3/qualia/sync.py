from __future__ import annotations

from threading import Event
from time import time
from typing import cast, Optional

from qualia.buffer import ParseProcess
from qualia.models import View, NodeId, Cursors, LastSync, Tree, Li
from qualia.services.realtime import Realtime
from qualia.services.utils.realtime_utils import sync_with_realtime_db
from qualia.utils.common_utils import get_key_val
from qualia.utils.sync_utils import sync_with_db


def sync_buffer(buffer_lines: Li, main_id: NodeId, last_sync: LastSync, cursors: Cursors,
                transposed: bool, realtime_session: Realtime, git_sync_event: Event) -> View:
    if main_id in last_sync:
        main_view, changes = ParseProcess().process_lines(buffer_lines, main_id, last_sync, cursors, transposed)
        realtime_data = sync_with_db(main_view, changes, last_sync, cursors, transposed, realtime_session.others_online)
        sync_with_realtime_db(realtime_data, realtime_session)
        if changes and realtime_session.last_broadcast_recieve_time < time() - 15:
            git_sync_event.set()
    else:
        main_view = View(main_id, cast(Optional[Tree],
                                       get_key_val(main_id, cursors.transposed_views if transposed else cursors.views,
                                                   False)) or {})
    return main_view
