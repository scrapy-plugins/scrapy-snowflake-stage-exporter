# pylint: disable=unused-argument,protected-access
import os
import re
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, patch

from snowflake_stage_exporter import SnowflakeStageExporter


@contextmanager
def make_test_exporter(table_path, **kwargs):
    def _put_file(fpath, stage, prefix):
        return prefix + "/" + os.path.basename(fpath)

    with patch(
        "snowflake_stage_exporter.SnowflakeStageExporter._put_file",
        Mock(side_effect=_put_file),
    ), patch(
        "snowflake_stage_exporter.SnowflakeStageExporter.create_connection", MagicMock()
    ), SnowflakeStageExporter(
        "user", "pass", "account", table_path, **kwargs
    ) as exporter:
        exporter._instance_ms = "INSTANCE_MS"
        yield exporter


def mock_calls_get_sql(calls):
    return [
        (re.sub(r"\n+\s+", " ", call.args[0]).strip(), *call.args[1:]) for call in calls
    ]
