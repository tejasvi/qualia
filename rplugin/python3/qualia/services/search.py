from __future__ import annotations

from json import loads
from typing import Iterable, cast

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32

from qualia.config import _FZF_LINE_DELIMITER
from qualia.models import NodeId, Cursors
from qualia.utils.common_utils import Database, get_node_content, normalized_search_prefixes


def matching_nodes_content(search_keywords: Iterable[str]) -> list[str]:
    with Database() as cursors:
        if search_keywords:
            matching_content_list = []
            for node_id_bytes in cursors.content.iternext(values=False):
                node_id: NodeId = node_id_bytes.decode()
                bloom_filter_data: bytes = cursors.bloom_filters.get(node_id_bytes)
                bloom_filter = BloomFilter.loads(bloom_filter_data) if bloom_filter_data else set_bloom_filter(
                    node_id, get_node_content(cursors, node_id), cursors)
                if all((bloom_filter.might_contain(search_keyword) for search_keyword in search_keywords)):
                    matching_content_list.append(fzf_input_line(node_id, get_node_content(cursors, node_id)))
        else:
            matching_content_list = [fzf_input_line(node_id_bytes.decode(), loads(content_bytes.decode()))
                                     for node_id_bytes, content_bytes in cursors.content]
    return matching_content_list


def fzf_input_line(node_id: NodeId, content: list[str]) -> str:
    return cast(str, node_id) + _FZF_LINE_DELIMITER + ' '.join(content)


def set_bloom_filter(node_id: NodeId, content_lines: list[str], cursors: Cursors):
    bloom_filter = BloomFilter(expected_insertions=100, err_rate=1, strategy=MURMUR128_MITZ_32)
    string = '\n'.join(content_lines)
    prefixes = normalized_search_prefixes(string)
    for prefix in prefixes:
        bloom_filter.put(prefix)
    cursors.bloom_filters.put(node_id.encode(), bloom_filter.dumps())
    return bloom_filter
