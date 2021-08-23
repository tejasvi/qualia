from __future__ import annotations

from bloomfilter import BloomFilter
from bloomfilter.bloomfilter_strategy import MURMUR128_MITZ_32
from lmdb import Cursor

from qualia import FZF_DELIMITER
from qualia.models import NodeId
from qualia.utils import Database, get_key_val, normalized_prefixes


def save_bloom_filter(node_id: NodeId, content_lines: list[str], bloom_filters_cursor: Cursor):
    bloom_filter = BloomFilter(expected_insertions=100, err_rate=0.1, strategy=MURMUR128_MITZ_32)
    string = '\n'.join(content_lines)
    prefixes = normalized_prefixes(string)
    for prefix in prefixes:
        bloom_filter.put(prefix)
    bloom_filters_cursor.put(node_id.encode(), bloom_filter.dumps())


def matching_nodes_content(search_keywords: list[str]) -> list[str]:
    matching_content_list = []
    with Database() as cursors:
        for node_id_bytes, bloom_filter_data in cursors.bloom_filters:
            bloom_filter = BloomFilter.loads(bloom_filter_data)
            if all([bloom_filter.might_contain(search_keyword) for search_keyword in search_keywords]):
                node_id = node_id_bytes.decode()
                matching_content_list.append(FZF_DELIMITER.join([node_id] + get_key_val(node_id, cursors.content)))
    return matching_content_list
