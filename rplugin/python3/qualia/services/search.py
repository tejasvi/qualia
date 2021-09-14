from __future__ import annotations

from typing import Iterable, cast

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32

from qualia.config import _FZF_LINE_DELIMITER, ENCRYPT_DB
from qualia.models import NodeId, Cursors, Li
from qualia.utils.common_utils import Database, get_node_content_lines, normalized_search_prefixes, fernet, cursor_keys


def matching_nodes_content(search_keywords: Iterable[str]) -> list[str]:
    with Database() as cursors:
        if search_keywords:
            matching_content_list = []
            cursors.content.first()
            for node_id in cursor_keys(cursors.content):
                node_id = cast(NodeId, node_id)
                bloom_filter_data: bytes = cursors.bloom_filters.get(node_id.encode())
                bloom_filter = BloomFilter.loads(fernet.decrypt(bloom_filter_data) if ENCRYPT_DB else
                                                 bloom_filter_data) if bloom_filter_data else set_bloom_filter(
                    node_id, get_node_content_lines(cursors, node_id), cursors)
                if all((bloom_filter.might_contain(search_keyword) for search_keyword in search_keywords)):
                    matching_content_list.append(fzf_input_line(node_id,
                                                                get_node_content_lines(cursors, node_id)))
        else:
            cursors.content.first()
            matching_content_list = [
                fzf_input_line(cast(NodeId, node_id), get_node_content_lines(cursors, cast(NodeId, node_id))) for
                node_id in
                cursor_keys(cursors.content)]
    return matching_content_list


def fzf_input_line(node_id: NodeId, content: list[str]) -> str:
    return cast(str, node_id) + _FZF_LINE_DELIMITER + ' '.join(content)


def set_bloom_filter(node_id: NodeId, content_lines: Li, cursors: Cursors):
    bloom_filter = BloomFilter(expected_insertions=100, err_rate=0.1, strategy=MURMUR128_MITZ_32)
    string = '\n'.join(content_lines)
    prefixes = normalized_search_prefixes(string)
    for prefix in prefixes:
        bloom_filter.put(prefix)
    bloom_filter_bytes = bloom_filter.dumps()
    if ENCRYPT_DB:
        bloom_filter_bytes = fernet.encrypt(bloom_filter_bytes)
    cursors.bloom_filters.put(node_id.encode(), bloom_filter_bytes)
    return bloom_filter
