import pytest

from snowflake_stage_exporter.utils import chunk


@pytest.mark.parametrize(
    "iterable, n, result",
    [
        (range(0), 4, []),
        (range(1), 1, [(0,)]),
        (range(6), 3, [(0, 1, 2), (3, 4, 5)]),
        (range(6), 4, [(0, 1, 2, 3), (4, 5)]),
        (range(6), 6, [tuple(range(6))]),
    ],
)
def test_chunk(iterable, n, result):
    assert list(chunk(iterable, n)) == result
