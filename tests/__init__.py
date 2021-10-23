# pylint: disable=unused-argument,protected-access
import os
import re
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, patch

from snowflake_stage_exporter import SnowflakeStageExporter


@contextmanager
def make_test_exporter(table_path, patch_put=True, **kwargs):
    def _put_file(fpath, stage, prefix):
        return prefix + "/" + os.path.basename(fpath)

    with patch("snowflake.connector", MagicMock()), SnowflakeStageExporter(
        "user", "pass", "account", table_path, **kwargs
    ) as exporter:
        if patch_put:
            exporter._put_file = _put_file
        exporter._instance_ms = "INSTANCE_MS"
        yield exporter


def read_table_buffer(exporter, table_path):
    buffer = exporter._table_buffers[table_path]
    buffer.flush()
    with open(buffer.name, encoding="utf8") as f:
        return f.read()


def mock_calls_get_sql(calls):
    cleaned = []
    for call in calls:
        try:
            value = (re.sub(r"\n+\s+", " ", call.args[0]).strip(), *call.args[1:])
        except Exception:  # pylint: disable=broad-except
            value = call
        cleaned.append(value)
    return cleaned
