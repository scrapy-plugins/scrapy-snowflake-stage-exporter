import json
import logging
import os
import time
import typing
from contextlib import AbstractContextManager
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict, Iterable, Literal

import snowflake.connector
from itemadapter import ItemAdapter

from .utils import chunk

logger = logging.getLogger(__name__)


ExporterEvent = Literal["finish", "flush", "never"]


class SnowflakeStageExporter(AbstractContextManager):

    default_typemap = {
        dict: "OBJECT",
        list: "ARRAY",
        int: "NUMBER",
        float: "NUMBER",
        bool: "BOOLEAN",
        str: "VARCHAR",
    }

    def __init__(
        self,
        user: str,
        password: str,
        account: str,
        table_path: str,
        connection_kwargs: Dict[str, Any] = None,
        stage: str = "@~",
        stage_path: str = "{table_path}/{instance_ms}_{batch_n}.jl",
        max_file_size: int = 2 ** 30,
        predefined_column_types: Dict[str, Dict[str, str]] = None,
        ignore_unexpected_fields: bool = True,
        allow_varying_value_types: bool = False,
        create_tables_on: ExporterEvent = "finish",
        populate_tables_on: ExporterEvent = "finish",
        clear_stage_on: ExporterEvent = "never",
    ):
        self.max_file_size = max_file_size
        self.table_path = table_path
        self.stage = stage
        self.stage_path = stage_path
        self.create_tables_on = create_tables_on
        self.populate_tables_on = populate_tables_on
        self.clear_stage_on = clear_stage_on
        self.allow_varying_value_types = allow_varying_value_types
        self.predefined_column_types = predefined_column_types or {}
        self.ignore_unexpected_fields = ignore_unexpected_fields
        self.instance_ms = int(time.time() * 1000)

        exporter_events = typing.get_args(ExporterEvent)
        for attr in ("create_tables_on", "populate_tables_on", "clear_stage_on"):
            val = getattr(self, attr)
            if val not in exporter_events:
                raise ValueError(
                    f"unexpected value ({val!r}) for {attr!r}, expected one of {exporter_events!r}"
                )

        self.conn = self.create_connection(
            user=user,
            password=password,
            account=account,
            **(connection_kwargs or {}),
        )
        self.table_buffers: Dict = {}  # {table_path: tmp_buffer_file}
        self.exported_fpaths: Dict = {}  # {table_path: [exported_fpath_in_stage, ...]}
        self.recorded_field_types: Dict = {}  # {table_path: {field: set({cls, ...})}}

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_connection(self, **connection_kwargs):
        return snowflake.connector.connect(**connection_kwargs)

    def close(self):
        self.conn.close()

    def export_item(self, item: Any, **extra_params) -> None:
        item_dict = ItemAdapter(item).asdict()

        table_path = self.table_for_item(item_dict, **extra_params)
        if table_path not in self.table_buffers:
            logger.info("Creating buffer for %r", table_path)
            self.table_buffers[table_path] = NamedTemporaryFile("wb")

        self.record_field_types(table_path, item_dict)

        line = json.dumps(item_dict) + "\n"
        self.table_buffers[table_path].write(line.encode("utf8"))
        if self.table_buffers[table_path].tell() >= self.max_file_size:
            self.flush_table_buffer(table_path)

    def record_field_types(self, table_path: str, item_dict):
        recorded = self.recorded_field_types.setdefault(table_path, {})
        for k, v in item_dict.items():
            if v is not None:
                recorded.setdefault(k, set()).add(type(v))

    def table_for_item(self, item_dict: str, **extra_params) -> str:
        return self.table_path.format(**extra_params, item=item_dict)

    def fpath_for_table(self, table_path: str, batch_n: int) -> str:
        return self.stage_path.format(
            table_path=table_path,
            instance_ms=self.instance_ms,
            batch_n=batch_n,
        )

    def flush_table_buffer(self, table_path: str) -> None:
        # XXX: there is no straightforward way to provide a filename via PUT
        # statement, so as a workaround we symlink our file into a temporary
        # directory with a desired filename.
        tmp_file = self.table_buffers[table_path]
        tmp_file.flush()
        batch_n = len(self.exported_fpaths.setdefault(table_path, [])) + 1
        with TemporaryDirectory() as tempdir:
            fpath = self.fpath_for_table(table_path, batch_n)
            logger.info(
                "Flushing buffer of %r to %r (batch %d)", table_path, fpath, batch_n
            )
            prefix, fname = fpath.rsplit("/", 1)
            symlink_path = os.path.join(tempdir, fname)
            os.symlink(tmp_file.name, symlink_path)
            cursor = self.conn.cursor().execute(
                f"PUT 'file://{symlink_path}' '{self.stage}/{prefix}'"
            )
            fpath = prefix + "/" + cursor.fetchone()[1]
        tmp_file.close()
        self.table_buffers.pop(table_path)
        self.exported_fpaths[table_path].append(fpath)

        if self.create_tables_on == "flush":
            self.create_table(table_path)
        if self.populate_tables_on == "flush":
            self.populate_table(table_path)
        if self.clear_stage_on == "flush":
            self.clear_stage([fpath])

    def flush_all_table_buffers(self) -> None:
        for table_path in list(self.table_buffers):
            self.flush_table_buffer(table_path)

    def get_column_types(self, table_path: str) -> Dict[str, str]:
        columns = self.predefined_column_types.get(table_path, {})
        if columns and self.ignore_unexpected_fields:
            return columns

        for colname, types in self.recorded_field_types.get(table_path, {}).items():
            if colname in columns:
                continue
            if len(types) == 1:
                type_ = next(iter(types))
                coltype = self.default_typemap.get(type_)
                if not coltype:
                    logger.error(
                        "Did not find an appropriate mapping from python type (%r)"
                        " to snowflake column type. Only basic JSON serializable types are supported."
                        "Field %r, skipping.",
                        type_,
                        colname,
                    )
                    continue
            else:
                if self.allow_varying_value_types:
                    coltype = "VARIANT"
                else:
                    logger.error(
                        "Multiple types encountered on values of the field %r: %r. Skipping.",
                        colname,
                        types,
                    )
                    continue
            columns[colname] = coltype
        return columns

    def create_table(self, table_path: str) -> None:
        logger.info("Creating table %r", table_path)
        cols = ", ".join(
            [
                f"{col} {type_}"
                for col, type_ in self.get_column_types(table_path).items()
            ]
        )
        self.conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table_path} ({cols})")

    def populate_table(self, table_path: str) -> None:
        all_fpaths = self.exported_fpaths[table_path]
        logger.info("Populating table %r", table_path)
        for fpaths in chunk(all_fpaths, 1000):
            coltypes = self.get_column_types(table_path)
            cols = ", ".join(coltypes)
            json_select = ", ".join(f"$1:{field}" for field in coltypes)
            fpaths = ", ".join(f"'{fpath}'" for fpath in fpaths)
            self.conn.cursor().execute(
                f"""
                COPY INTO {table_path} ({cols})
                    FROM (SELECT {json_select} FROM {self.stage})
                    FILE_FORMAT = (TYPE = JSON)
                    FILES = ({fpaths})
                """
            )

    def create_all_tables(self) -> None:
        for table_path in self.exported_fpaths:
            self.create_table(table_path)

    def populate_all_tables(self) -> None:
        for table_path in self.exported_fpaths:
            self.populate_table(table_path)

    def clear_stage(self, fpaths: Iterable[str] = None):
        if fpaths is None:
            logger.info("Removing all staged files")
            fpaths = [fpath for fpaths in self.exported_fpaths for fpath in fpaths]
        elif fpaths:
            logger.info("Removing staged files: %r", fpaths)
        if fpaths:
            self.conn.cursor().executemany(
                "REMOVE %s", [(f"{self.stage}/{fpath}",) for fpath in fpaths]
            )

    def finish_export(self) -> None:
        self.flush_all_table_buffers()
        if self.create_tables_on == "finish":
            self.create_all_tables()
        if self.populate_tables_on == "finish":
            self.populate_all_tables()
        if self.clear_stage_on == "finish":
            self.clear_stage()
