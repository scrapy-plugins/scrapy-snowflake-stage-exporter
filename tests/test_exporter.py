# pylint: disable=redefined-outer-name,protected-access,no-member,unspecified-encoding
import fnmatch
from unittest.mock import call

import pytest
import snowflake  # type: ignore

from snowflake_stage_exporter import SnowflakeStageExporterError
from tests import make_test_exporter, mock_calls_get_sql, read_table_buffer


def test_basic_flow():
    with make_test_exporter("{something}_{item[myfield]}") as exporter:
        assert exporter.export_item({"myfield": 1}, something="foo") == "foo_1"
        assert exporter.export_item({"myfield": 2}, something="bar") == "bar_2"
        exporter.export_item({"myfield": 2, "x": {}}, something="bar")

        assert set(exporter._table_buffers.keys()) == {"foo_1", "bar_2"}
        assert read_table_buffer(exporter, "foo_1") == '{"myfield": 1}\n'
        assert (
            read_table_buffer(exporter, "bar_2")
            == '{"myfield": 2}\n{"myfield": 2, "x": {}}\n'
        )

        exporter.finish_export()
        assert exporter._table_buffers == {}

        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
            # NOTE: following expectations are generated via tests/record_sql.py
            ("CREATE TABLE IF NOT EXISTS foo_1 (myfield NUMBER)",),
            ("CREATE TABLE IF NOT EXISTS bar_2 (myfield NUMBER, x OBJECT)",),
            (
                "COPY INTO foo_1 (myfield) FROM (SELECT $1:myfield FROM @~) FILE_FORMAT = "
                "(TYPE = JSON) FILES = ('foo_1/INSTANCE_MS_1.jl')",
            ),
            (
                "COPY INTO bar_2 (myfield, x) FROM (SELECT $1:myfield, $1:x FROM @~) "
                "FILE_FORMAT = (TYPE = JSON) FILES = ('bar_2/INSTANCE_MS_1.jl')",
            ),
        ]
    exporter.conn.close.assert_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(create_tables_on=""),
        dict(clear_stage_on="neyvar"),
        dict(stage="x"),
        dict(stage="x/"),
        dict(stage="@x/z"),
        dict(predefined_column_types={"test": {}}),
        dict(predefined_column_types={"test": None}),
        dict(predefined_column_types={"test": {}, "test2": None}),
    ],
)
def test_bad_configuration(kwargs):
    with pytest.raises(ValueError):
        with make_test_exporter("table", **kwargs):
            pass


def test_clear_stage():
    with make_test_exporter("{table}", clear_stage_on="finish") as exporter:
        assert exporter.export_item({"myfield": 1}, table="a")
        assert exporter.export_item({"myfield": 1}, table="b")
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
            ("CREATE TABLE IF NOT EXISTS a (myfield NUMBER)",),
            ("CREATE TABLE IF NOT EXISTS b (myfield NUMBER)",),
            (
                "COPY INTO a (myfield) FROM (SELECT $1:myfield FROM @~) FILE_FORMAT = (TYPE "
                "= JSON) FILES = ('a/INSTANCE_MS_1.jl')",
            ),
            (
                "COPY INTO b (myfield) FROM (SELECT $1:myfield FROM @~) FILE_FORMAT = (TYPE "
                "= JSON) FILES = ('b/INSTANCE_MS_1.jl')",
            ),
            ("REMOVE %s", [("@~/a/INSTANCE_MS_1.jl",), ("@~/b/INSTANCE_MS_1.jl",)]),
        ]
    with make_test_exporter("table") as exporter:
        exporter.clear_stage(["aa", "bb"])
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
            ("REMOVE %s", [("@~/aa",), ("@~/bb",)])
        ]


def test_chunking():
    buffer_sizes = {}
    with make_test_exporter("table", max_file_size=2 ** 10 * 100) as exporter:
        # patch `exporter.flush_table_buffer` to track buffer sizes upon flush
        orig = exporter.flush_table_buffer

        def flush_table_buffer(table_path):
            buffer = exporter._table_buffers[table_path]
            buffer_sizes[buffer.name] = buffer.tell()
            return orig(table_path)

        exporter.flush_table_buffer = flush_table_buffer

        large_string = " " * 5_000
        for _ in range(100):
            exporter.export_item({"a": large_string})
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
            ("CREATE TABLE IF NOT EXISTS table (a VARCHAR)",),
            (
                "COPY INTO table (a) FROM (SELECT $1:a FROM @~) FILE_FORMAT = (TYPE = JSON) "
                "FILES = ('table/INSTANCE_MS_1.jl', 'table/INSTANCE_MS_2.jl', "
                "'table/INSTANCE_MS_3.jl', 'table/INSTANCE_MS_4.jl', "
                "'table/INSTANCE_MS_5.jl')",
            ),
        ]
    assert list(buffer_sizes.values()) == [105210, 105210, 105210, 105210, 80160]


def test_put_file():
    # pylint: disable=no-value-for-parameter
    with make_test_exporter("table", patch_put=False) as exporter:
        exporter.export_item({"a": 1})
        exporter.finish_export()
        sql = mock_calls_get_sql(exporter.conn.cursor().mock_calls)[0][0]
        assert fnmatch.fnmatch(
            sql,
            "PUT 'file:///tmp/*/INSTANCE_MS_1.jl' '@~/table'",
        )
        assert (
            exporter.conn.cursor().execute().fetchone().__getitem__.call_args_list
            == [call(1)]
        )


def test_predefined_fields():
    types = {
        "aa": {
            "a": "VARCHAR",
            "b": "NUMBER",
            "c": "VARIANT",
        },
    }
    with make_test_exporter("{table}", predefined_column_types=types) as exporter:
        for table in ("aa", "bb"):
            exporter.export_item({"a": 1, "b": 2, "c": "3", "d": 4}, table=table)
            exporter.export_item({"a": 1.1, "b": 2, "c": 3, "e": {}}, table=table)
            assert (
                read_table_buffer(exporter, table)
                == '{"a": 1, "b": 2, "c": "3", "d": 4}\n{"a": 1.1, "b": 2, "c": 3, "e": {}}\n'
            )
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
            ("CREATE TABLE IF NOT EXISTS aa (a VARCHAR, b NUMBER, c VARIANT)",),
            ("CREATE TABLE IF NOT EXISTS bb (b NUMBER, d NUMBER, e OBJECT)",),
            (
                "COPY INTO aa (a, b, c) FROM (SELECT $1:a, $1:b, $1:c FROM @~) FILE_FORMAT = "
                "(TYPE = JSON) FILES = ('aa/INSTANCE_MS_1.jl')",
            ),
            (
                "COPY INTO bb (b, d, e) FROM (SELECT $1:b, $1:d, $1:e FROM @~) FILE_FORMAT = "
                "(TYPE = JSON) FILES = ('bb/INSTANCE_MS_1.jl')",
            ),
        ]


@pytest.mark.parametrize(
    "ignore, expect_sqls",
    [
        (
            False,
            [
                ("CREATE TABLE IF NOT EXISTS table (a VARCHAR, b NUMBER)",),
                (
                    "COPY INTO table (a, b) FROM (SELECT $1:a, $1:b FROM @~) FILE_FORMAT = (TYPE "
                    "= JSON) FILES = ('table/INSTANCE_MS_1.jl')",
                ),
            ],
        ),
        (
            True,
            [
                ("CREATE TABLE IF NOT EXISTS table (a VARCHAR)",),
                (
                    "COPY INTO table (a) FROM (SELECT $1:a FROM @~) FILE_FORMAT = (TYPE = JSON) "
                    "FILES = ('table/INSTANCE_MS_1.jl')",
                ),
            ],
        ),
    ],
)
def test_ignore_unexpected_fields(ignore, expect_sqls):
    with make_test_exporter(
        "table",
        predefined_column_types={"table": {"a": "VARCHAR"}},
        ignore_unexpected_fields=ignore,
    ) as exporter:
        exporter.export_item({"a": 1, "b": 2})
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == expect_sqls


@pytest.mark.parametrize("value", [object(), b"", 5j])
def test_non_serializable(value):
    with pytest.raises(TypeError):
        with make_test_exporter("table") as exporter:
            exporter.export_item({"a": value})


def test_no_table_manipulation():
    with make_test_exporter(
        "table", create_tables_on="never", populate_tables_on="never"
    ) as exporter:
        exporter.export_item({"a": 1})
        exporter.finish_export()
    assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == []


def test_dynamic_fields():
    with make_test_exporter("table", allow_varying_value_types=True) as exporter:
        exporter.export_item(
            {"a": 1, "b": 2.1, "c": True, "d": None, "e": "", "f": [], "g": {}, "h": 0}
        )
        exporter.export_item({"h": ""})
        exporter.finish_export()
    assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == [
        (
            "CREATE TABLE IF NOT EXISTS table (a NUMBER, b FLOAT, c BOOLEAN, e VARCHAR, "
            "f ARRAY, g OBJECT, h VARIANT)",
        ),
        (
            "COPY INTO table (a, b, c, e, f, g, h) FROM (SELECT $1:a, $1:b, $1:c, $1:e, "
            "$1:f, $1:g, $1:h FROM @~) FILE_FORMAT = (TYPE = JSON) FILES = "
            "('table/INSTANCE_MS_1.jl')",
        ),
    ]


@pytest.mark.parametrize(
    "allow, expect_sqls",
    [
        (
            True,
            [
                ("CREATE TABLE IF NOT EXISTS table (a VARIANT, b VARIANT, c ARRAY)",),
                (
                    "COPY INTO table (a, b, c) FROM (SELECT $1:a, $1:b, $1:c FROM @~) "
                    "FILE_FORMAT = (TYPE = JSON) FILES = ('table/INSTANCE_MS_1.jl')",
                ),
            ],
        ),
        (
            False,
            [
                ("CREATE TABLE IF NOT EXISTS table (c ARRAY)",),
                (
                    "COPY INTO table (c) FROM (SELECT $1:c FROM @~) FILE_FORMAT = (TYPE = JSON) "
                    "FILES = ('table/INSTANCE_MS_1.jl')",
                ),
            ],
        ),
    ],
)
def test_allow_varying_value_types(allow, expect_sqls):
    with make_test_exporter("table", allow_varying_value_types=allow) as exporter:
        exporter.export_item({"a": 1, "b": "", "c": None})
        exporter.export_item({"a": 1.1, "b": 0, "c": []})
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == expect_sqls


def test_all_fields_skipped():
    with make_test_exporter("table") as exporter:
        exporter.export_item({"a": 1})
        exporter.export_item({"a": "1.1"})
        exporter.export_item({})
        with pytest.raises(SnowflakeStageExporterError):
            exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == []


def test_stage_path():
    with make_test_exporter(
        "table", stage="@x", stage_path="AA/BB/cc/{table_path}/{instance_ms}/{batch_n}"
    ) as exporter:
        exporter.export_item({"a": 1})
        exporter.finish_export()
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls)[-1] == (
            "COPY INTO table (a) FROM (SELECT $1:a FROM @x) FILE_FORMAT = (TYPE = JSON) "
            "FILES = ('AA/BB/cc/table/INSTANCE_MS/1')",
        )


@pytest.mark.parametrize(
    "flush_or_finish, expect_sqls",
    [
        (
            "flush",
            [
                ("CREATE TABLE IF NOT EXISTS table (a NUMBER)",),
                (
                    "COPY INTO table (a) FROM (SELECT $1:a FROM @~) FILE_FORMAT = (TYPE = JSON) "
                    "FILES = ('table/INSTANCE_MS_1.jl')",
                ),
                ("REMOVE %s", [("@~/table/INSTANCE_MS_1.jl",)]),
                ("CREATE TABLE IF NOT EXISTS table (a NUMBER)",),
                (
                    "COPY INTO table (a) FROM (SELECT $1:a FROM @~) FILE_FORMAT = (TYPE = JSON) "
                    "FILES = ('table/INSTANCE_MS_1.jl', 'table/INSTANCE_MS_2.jl')",
                ),
                ("REMOVE %s", [("@~/table/INSTANCE_MS_2.jl",)]),
            ],
        ),
        (
            "finish",
            [
                ("CREATE TABLE IF NOT EXISTS table (a NUMBER)",),
                (
                    "COPY INTO table (a) FROM (SELECT $1:a FROM @~) FILE_FORMAT = (TYPE = JSON) "
                    "FILES = ('table/INSTANCE_MS_1.jl', 'table/INSTANCE_MS_2.jl')",
                ),
                (
                    "REMOVE %s",
                    [("@~/table/INSTANCE_MS_1.jl",), ("@~/table/INSTANCE_MS_2.jl",)],
                ),
            ],
        ),
    ],
)
def test_on_flush_or_finish(flush_or_finish, expect_sqls):
    with make_test_exporter(
        "table",
        create_tables_on=flush_or_finish,
        populate_tables_on=flush_or_finish,
        clear_stage_on=flush_or_finish,
    ) as exporter:
        exporter.export_item({"a": 1})
        exporter.flush_all_table_buffers()
        exporter.export_item({"a": 1})
        exporter.flush_all_table_buffers()
        if flush_or_finish == "finish":
            exporter.finish_export()
        calls_pre_finish = list(exporter.conn.cursor().mock_calls)
        assert mock_calls_get_sql(exporter.conn.cursor().mock_calls) == expect_sqls
        if flush_or_finish == "flush":
            exporter.finish_export()
            # ensure nothing else required connection call
            assert exporter.conn.cursor().mock_calls == calls_pre_finish


def test_connection_kwargs():
    with make_test_exporter("table", connection_kwargs={"role": "PUBLIC"}):
        snowflake.connector.connect.assert_called_with(
            user="user", password="pass", account="account", role="PUBLIC"
        )
    with pytest.raises(TypeError):
        # got multiple values for keyword argument 'user'
        with make_test_exporter("table", connection_kwargs={"user": "a"}):
            pass
