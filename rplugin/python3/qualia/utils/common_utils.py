from __future__ import annotations

import itertools
from base64 import urlsafe_b64encode, b64decode, b64encode, b32decode, b32encode
from bisect import bisect_left, insort
from hashlib import sha256
from json import dumps
from logging import getLogger
from math import ceil
from os import PathLike
from re import split
from subprocess import run, CalledProcessError
from threading import Thread
from time import time_ns, sleep
from traceback import format_exception
from typing import Union, cast, Iterable, Callable, TYPE_CHECKING, Optional
from uuid import UUID, uuid4

from qualia.config import _LOGGER_NAME, _TRANSPOSED_FILE_PREFIX, \
    _CONFLICT_MARKER, _ENCRYPTION_KEY_FILE, _ENCRYPTION_USED, _SHORT_ID_STORE_BYTES, DEBUG, _GIT_FOLDER
from qualia.models import NodeId, CustomCalledProcessError, El, Li, ShortId, KeyNotFoundError
from qualia.services.backup import removesuffix

if TYPE_CHECKING:
    from pynvim import Nvim


def get_time_uuid() -> NodeId:
    from secrets import token_bytes
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
    live_logger.debug(f"Git:\n{stdout}\n")
    return stdout


class _LiveLogger:
    def __init__(self) -> None:
        self._nvim: Optional[Nvim] = None
        self._logger = getLogger(_LOGGER_NAME)
        self._visible_levels = {"info", "warning", "error", "critical"}

    def __getattr__(self, name) -> Callable[[object], None]:
        def wrapper(msg: object) -> None:
            msg = str(msg)
            if self._nvim is not None and (DEBUG or name in self._visible_levels):
                try:
                    self._nvim.async_call(self._nvim.out_write, msg + '\n')
                except Exception as e:
                    msg += ('\n' + exception_traceback(e))
            getattr(self._logger, name)(msg)

        return wrapper

    def attach_nvim(self, nvim: Nvim):
        self._nvim = nvim


live_logger = _LiveLogger()


def exception_traceback(e: BaseException) -> str:
    return '\n' + '\n'.join(format_exception(None, e, e.__traceback__))


def ordered_data_hash(data: Union[Li, list[NodeId]]) -> str:
    return urlsafe_b64encode(sha256(data if isinstance(data, bytes) else dumps(data).encode()).digest()).decode()


def children_data_hash(data: Iterable[NodeId]) -> str:
    return ordered_data_hash(sorted(data))


def normalized_search_prefixes(string: str) -> set[str]:
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}


class StartLoggedThread(Thread):
    def __init__(self, target: Callable, name: str, delay_seconds: float):
        def logged_target() -> None:
            sleep(delay_seconds)
            try:
                target()
            except BaseException as e:
                live_logger.critical("Error in thread " + name + "\n" + exception_traceback(e))
                raise e

        super().__init__(target=logged_target, name=name)
        self.start()


def get_uuid() -> NodeId:
    return cast(NodeId, str(uuid4()))


def open_write_lf(file_path: Union[str, bytes, PathLike], prevent_overwrite: bool, lines: list[str]) -> None:
    with open(file_path, 'x' if prevent_overwrite else 'w', newline='\n') as file:
        file.write('\n'.join(lines) + '\n')


if _ENCRYPTION_USED:
    from cryptography.fernet import Fernet

    if not _ENCRYPTION_KEY_FILE.exists():
        _ENCRYPTION_KEY_FILE.write_bytes(Fernet.generate_key())
    fernet = Fernet(_ENCRYPTION_KEY_FILE.read_bytes())
else:
    from qualia.models import AbstractFernet as AbstractFernet

    _abstract_fernet = AbstractFernet(b"dummy_key")  # noqa[assignment]
    if TYPE_CHECKING:
        from cryptography.fernet import Fernet

        _abstract_fernet = cast(Fernet, _abstract_fernet)
    fernet = _abstract_fernet


def decrypt_lines(encrypted_lines: El) -> Li:
    return cast(Li, fernet.decrypt(encrypted_lines[0].encode()).decode().split('\n'))


def encrypt_lines(unencrypted_lines: Li) -> El:
    return cast(El, [fernet.encrypt('\n'.join(unencrypted_lines).encode()).decode()])


def trigger_buffer_change(nvim):
    # type:(Nvim) -> None
    nvim.async_call(nvim.command,
                    """execute (expand("%:p")[-5:] ==? ".q.md" && mode() !=# "t") ? "TriggerSync 1" : "" """,
                    async_=True)


def _decompact_encoded_string(string: str, unpadded_length: int, padding: int):
    return string.rjust(unpadded_length, 'A') + "=" * padding


def compact_base32_decode(string: str) -> bytes:
    # Base32 stores 5 bits per letter. 00000 is represented as 'A'. The value encoded is in bytes (multiple of 8bits)
    # The length of encoded value will have multiple of 8 characters (8*5 bits representing 5 byte value)
    unpadded_length = ceil(_SHORT_ID_STORE_BYTES * 8 / 5)
    padding = (8 - unpadded_length % 8) % 8  # Need to be exact
    byte_string = b32decode(_decompact_encoded_string(string, unpadded_length, padding), casefold=True)
    return byte_string


def compact_base32_encode(byte_string: bytes) -> str:
    return b32encode(byte_string).decode().rstrip("=").lstrip('A') or 'A'


def compact_base64_decode(string: str) -> bytes:
    # Base64 stores 6 bits per letter. 000000 is represented as 'A'
    unpadded_length = ceil(_SHORT_ID_STORE_BYTES * 8 / 6)
    padding = 2  # Extra '=' padding is ignored
    byte_string = b64decode(_decompact_encoded_string(string, unpadded_length, padding), validate=True)
    return byte_string


def compact_base64_encode(byte_string: bytes) -> str:
    return b64encode(byte_string).decode().rstrip("=").lstrip('A') or 'A'


def buffer_id_decoder(buffer_id: ShortId) -> bytes:
    return compact_base32_decode(buffer_id)


def buffer_id_encoder(buffer_id_bytes: bytes) -> ShortId:
    return cast(ShortId, compact_base32_encode(buffer_id_bytes))


absent_node_content_lines = cast(Li, [''])


def removeprefix(input_string: str, suffix: str) -> str:
    # in 3.9 str.removeprefix
    if suffix and input_string.startswith(suffix):
        return input_string[len(suffix):]
    return input_string


class InvalidBufferNodeIdError(KeyNotFoundError):
    def __init__(self, buffer_node_id: ShortId):
        if buffer_node_id != 'A':
            live_logger.critical(f'Node ID "{buffer_node_id}" not found')
        super().__init__(buffer_node_id)


counter = itertools.count()