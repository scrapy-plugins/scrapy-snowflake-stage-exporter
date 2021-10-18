# pylint: disable=redefined-outer-name,protected-access,no-member,unspecified-encoding
from tests import assert_sqls, make_test_exporter


def test_exporter_basic():
    with make_test_exporter("{something}_{item[myfield]}") as exporter:
        assert exporter.export_item({"myfield": 1}, something="foo") == "foo_1"
        assert exporter.export_item({"myfield": 2}, something="bar") == "bar_2"
        exporter.export_item({"myfield": 2, "x": {}}, something="bar")

        assert set(exporter._table_buffers.keys()) == {"foo_1", "bar_2"}

        buffer_foo = exporter._table_buffers["foo_1"]
        buffer_foo.flush()
        with open(buffer_foo.name) as f:
            assert f.read() == '{"myfield": 1}\n'

        buffer_bar = exporter._table_buffers["bar_2"]
        buffer_bar.flush()
        with open(buffer_bar.name) as f:
            assert f.read() == '{"myfield": 2}\n{"myfield": 2, "x": {}}\n'

        exporter.finish_export()
        assert not exporter._table_buffers

        assert_sqls(
            exporter.conn.cursor().execute.mock_calls,
            [
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
            ],
        )
