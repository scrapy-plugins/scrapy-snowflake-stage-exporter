import pytest

from snowflake_stage_exporter.utils import chunk, normalize_identifier


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


@pytest.mark.parametrize(
    "value, result",
    [
        ("A.B. C ", "A_B_C"),
        ("", "_empty_e3b0c4"),
        ("91 $", "_91_$"),
        ("$", "_$"),
        ("   ", "_empty_0aad7d"),
        ("abc", "abc"),
        ("ABc", "ABc"),
        ("  AB;c =+ ", "AB_c"),
        ("  AB   c", "AB_c"),
    ],
)
def test_normalize_identifier(value, result):
    assert normalize_identifier(value) == result
