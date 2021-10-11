from os import environ
from sys import argv
from typing import Optional, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from qualia.models import NodeId, Li
    from qualia.database import Database
    from multiprocessing.connection import Connection


def get_descendant_preview_lines(node_id, db, transposed, separator_width, max_level):
    # type: (NodeId, Database, bool, int, int) ->  Li
    from qualia.models import Li

    descendant_preview_lines = []

    descendant_node_ids = db.get_node_descendants(node_id, transposed, True)
    if descendant_node_ids:
        descendant_preview_lines.append(separator_width * ('━' if transposed else '─'))

    stack = [(descendant_node_ids, 0)]
    while stack:
        cur_descendant_node_ids, level = stack.pop()
        for descendant_id in cur_descendant_node_ids:
            indent_spaces = " " * 4 * level
            sub_descendant_node_ids = db.get_node_descendants(descendant_id, transposed, True)
            has_other_ancestors = len(db.get_node_descendants(descendant_id, not transposed, False)) != 0
            descendant_content_lines = db.get_node_content_lines(descendant_id)

            descendant_preview_lines.append(indent_spaces
                                            + (('▶' if transposed else '‣')
                                               if (level == max_level and len(sub_descendant_node_ids) > 0)
                                               else ('●' if transposed else '•'))
                                            + ("ॱ" if has_other_ancestors else " ")
                                            + descendant_content_lines.pop())

            descendant_preview_lines.extend(indent_spaces + "  " + line for line in descendant_content_lines)

            if level < max_level:
                stack.append((sub_descendant_node_ids, level + 1))
    return cast(Li, descendant_preview_lines)


def _wrapped_lines_height(lines, width):
    # type:(Li, int)->int
    return sum(-(len(line) // -width) for line in lines)


def pad_lines(lines, width, min_height):
    # type:(Li, int, int)->Li
    wrapped_height = _wrapped_lines_height(lines, width)
    if wrapped_height < min_height:
        lines.extend('' for _ in range(min_height - wrapped_height))
    return lines


def preview_node(node_id, separator_width, output_height, depth):
    # type:( NodeId, int, int, int) -> Li
    from qualia.models import KeyNotFoundError, Li
    from qualia.database import Database

    min_content_height, min_children_height = output_height // 4, output_height // 2
    min_parents_height = output_height - min_children_height - min_content_height

    preview_lines = cast(Li, [])
    with Database() as db:
        try:
            preview_lines.extend(
                pad_lines(db.get_node_content_lines(node_id), separator_width, min_content_height))
        except KeyNotFoundError:
            raise Exception(f"Requested Node ID does not exist on the database used by ")
        for transposed, min_height in ((False, min_children_height), (True, min_parents_height)):
            preview_lines.extend(pad_lines(
                get_descendant_preview_lines(node_id, db, transposed, separator_width, depth - 1)
                , separator_width, min_height))
    return preview_lines


class InvalidParameters(Exception):
    pass


def parse_args() -> tuple[bool, str, int, int]:
    try:
        node_id_arg = argv[1]
        # from uuid import UUID
        # UUID(node_id_arg) # skip to shave 20ms
    except (IndexError, ValueError) as e:
        print(f"Improper usage. Pass Node ID as single argument (UUID hex format). Recieved argument list: {argv[1:]}"
              + str(e))
        raise InvalidParameters
    output_width = int(environ.get('FZF_PREVIEW_COLUMNS', 10)) - 1
    output_height = int(environ.get('FZF_PREVIEW_LINES', 20)) - 1
    try:
        attach_running_process = True if len(argv) == 3 and bool(int(argv[2])) else False
    except ValueError:
        print(f"Invalid arguments. Passed argument list: {argv=}")
        raise InvalidParameters
    return attach_running_process, node_id_arg, output_width, output_height


def connect_listener():
    # type: () -> Optional[Connection]
    from multiprocessing.connection import Client
    try:
        return Client(('localhost', 1200))
    except ConnectionRefusedError as e:
        print("Could not find qualia listening on port 1200\n" + str(e))
        return None
