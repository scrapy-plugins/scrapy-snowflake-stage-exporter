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
