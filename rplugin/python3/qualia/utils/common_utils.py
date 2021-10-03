from __future__ import annotations

from base64 import urlsafe_b64encode, b64decode, b64encode
from bisect import bisect_left, insort
from hashlib import sha256
from json import dumps
from logging import getLogger
from math import ceil
from os import PathLike
from re import split
from secrets import token_bytes
from subprocess import run, CalledProcessError
from threading import Thread
from time import time_ns
from traceback import format_exception
from typing import Union, cast, Iterable, Callable, IO, TYPE_CHECKING
from uuid import UUID, uuid4

from qualia.config import _GIT_FOLDER, _LOGGER_NAME, _TRANSPOSED_FILE_PREFIX, \
    _CONFLICT_MARKER, _ENCRYPTION_KEY_FILE, _ENCRYPTION_USED, _SHORT_ID_STORE_BYTES
from qualia.models import NodeId, CustomCalledProcessError, El, Li, BufferNodeId
from qualia.services.backup import removesuffix

if TYPE_CHECKING:
    from pynvim import Nvim


def get_time_uuid() -> NodeId:
    left_padded_time = (time_ns() // 10 ** 6).to_bytes(6, "big")
    id_bytes = left_padded_time + token_bytes(10)
    return cast(NodeId, str(UUID(bytes=id_bytes)))


def conflict(new_lines: Li, old_lines: Li) -> Li:
    if new_lines == old_lines:
        return new_lines
    else:
        # Prevent oscillating sync conflicts with _OrderedSet_esque merge (like children conflicts)
        conflicting_content_lines: list[Li] = []
        for content_lines in _splitlines_conflict_marker(old_lines) + _splitlines_conflict_marker(new_lines):
            content_lines.append(_CONFLICT_MARKER)
            idx = bisect_left(conflicting_content_lines, content_lines)
            if idx == len(conflicting_content_lines) or conflicting_content_lines[idx] != content_lines:
                insort(conflicting_content_lines, content_lines)

        merged_content_lines = cast(Li, [content_line for content_lines in conflicting_content_lines for content_line in
                                         content_lines])
        merged_content_lines.pop()  # Trailing _CONFLICT_MARKER
        return merged_content_lines


def _splitlines_conflict_marker(new_lines: Li) -> list[Li]:
    splitted_lines_list = []
    last_conflict_idx = 0
    for idx, line in enumerate(new_lines):
        if line == _CONFLICT_MARKER:
            splitted_lines_list.append(new_lines[last_conflict_idx: idx])
            last_conflict_idx = idx + 1
    splitted_lines_list.append(new_lines[last_conflict_idx:])
    return cast(list[Li], splitted_lines_list)


def file_name_to_file_id(full_name: str, extension: str) -> str:
    if full_name.endswith(extension):
        file_id = removesuffix(removeprefix(full_name, _TRANSPOSED_FILE_PREFIX), extension)
        return file_id
    else:
        raise ValueError


# @line_profiler_pycharm.profile


def cd_run_git_cmd(arguments: list[str]) -> str:
    try:
        result = run(["git"] + arguments, check=True, cwd=_GIT_FOLDER, capture_output=True, text=True)
    except CalledProcessError as e:
        raise CustomCalledProcessError(e)
    stdout = f"{result.stdout}{result.stderr}".strip()
    logger.debug(f"Git:\n{stdout}\n")
    return stdout


logger = getLogger(_LOGGER_NAME)


def exception_traceback(e: BaseException) -> str:
    return '\n' + '\n'.join(format_exception(None, e, e.__traceback__))


def ordered_data_hash(data: Union[Li, list[NodeId]]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def children_data_hash(data: Iterable[NodeId]) -> str:
    return ordered_data_hash(sorted(data))


def normalized_search_prefixes(string: str) -> set[str]:
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}


class StartLoggedThread(Thread):
    def __init__(self, target: Callable, name: str):
        def logged_target() -> None:
            try:
                target()
            except BaseException as e:
                logger.critical("Exception in thread " + name + "\n" + exception_traceback(e))
                raise e

        super().__init__(target=logged_target, name=name)
        self.start()


def get_uuid() -> NodeId:
    return cast(NodeId, str(uuid4()))


def open_write_lf(file_path: Union[str, bytes, PathLike], prevent_overwrite: bool) -> IO:
    return open(file_path, 'x' if prevent_overwrite else 'w', newline='\n')


if _ENCRYPTION_USED:
    from cryptography.fernet import Fernet

    fernet = Fernet(_ENCRYPTION_KEY_FILE.read_bytes())
else:
    from qualia.models import AbstractFernet as AbstractFernet

    fernet = cast(Fernet, AbstractFernet(b"dummy_key"))  # noqa[assignment]


def decrypt_lines(encrypted_lines: El) -> Li:
    return cast(Li, fernet.decrypt(encrypted_lines[0].encode()).decode().split('\n'))


def encrypt_lines(unencrypted_lines: Li) -> El:
    return cast(El, [fernet.encrypt('\n'.join(unencrypted_lines).encode()).decode()])


def trigger_buffer_change(nvim):
    # type:(Nvim) -> None
    nvim.async_call(nvim.command,
                    """execute (expand("%:p")[-5:] ==? ".q.md" && mode() !=# "t") ? "TriggerSync" : "" """,
                    async_=True)


def buffer_id_decoder(buffer_id: BufferNodeId) -> bytes:
    # Base64 stores 6 bits per letter. 000000 is represented as 'A'
    return b64decode(buffer_id.rjust(ceil(_SHORT_ID_STORE_BYTES * 8 / 6), 'A') + "==")  # base65536.decode


absent_node_content_lines = cast(Li, [''])


def buffer_id_encoder(buffer_id_bytes: bytes) -> BufferNodeId:
    buffer_id = cast(BufferNodeId, b64encode(buffer_id_bytes).decode().rstrip("=").lstrip('A') or 'A')
    return buffer_id


def removeprefix(input_string: str, suffix: str) -> str:
    # in 3.9 str.removeprefix
    if suffix and input_string.startswith(suffix):
        return input_string[len(suffix):]
    return input_string
