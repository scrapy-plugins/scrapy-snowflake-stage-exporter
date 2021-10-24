import re
from hashlib import sha256
from typing import Iterable


def chunk(iterable: Iterable, n: int):
    current_chunk = []
    for el in iterable:
        current_chunk.append(el)
        if len(current_chunk) == n:
            yield tuple(current_chunk)
            current_chunk = []
    if current_chunk:
        yield tuple(current_chunk)


def normalize_identifier(value: str) -> str:
    orig_value = value
    value = re.sub(r"(?i)[^a-z\d_$]+", " ", value).strip()
    if not value:
        return "_empty_" + sha256(orig_value.encode("utf8")).hexdigest()[:6]
    value = re.sub(r" +", "_", value)
    if not re.match(r"(?i)[_a-z]", value):
        value = "_" + value
    return value
