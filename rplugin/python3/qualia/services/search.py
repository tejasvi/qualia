from __future__ import annotations

from typing import Iterable, cast

from qualia.config import _FZF_LINE_DELIMITER
from qualia.database import Database
from qualia.models import NodeId


def matching_nodes_content(search_keywords: Iterable[str]) -> list[str]:
    with Database() as db:
        if search_keywords:
            matching_content_list = []
            for node_id in db.get_node_ids():
                bloom_filter = db.get_set_bloom_filter(node_id)
                if all((bloom_filter.might_contain(search_keyword) for search_keyword in search_keywords)):
                    matching_content_list.append(fzf_input_line(node_id, db.get_node_content_lines(node_id), False))
        else:
            matching_content_list = [fzf_input_line(node_id, db.get_node_content_lines(node_id), False) for node_id in
                                     db.get_node_ids()]
    return matching_content_list


def fzf_input_line(node_id: NodeId, content: list[str], highlight: bool) -> str:
    content_line = ' '.join(content)
    if highlight:
        content_line = '\033[91m' + '\033[1m' + content_line + '\033[0m' * 2
    input_line = cast(str, node_id) + _FZF_LINE_DELIMITER + content_line
    return input_line
