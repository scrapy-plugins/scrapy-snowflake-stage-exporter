import json
import logging
import os
import time
import typing
from contextlib import AbstractContextManager
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Collection, Dict, Iterable, Literal

import snowflake.connector  # type: ignore
from itemadapter import ItemAdapter  # type: ignore

from .utils import chunk

logger = logging.getLogger(__name__)


ExporterEvent = Literal["finish", "flush", "never"]


class SnowflakeStageExporterError(Exception):
    pass


class SnowflakeStageExporter(AbstractContextManager):

    typemap = {
        dict: "OBJECT",
        list: "ARRAY",
        int: "NUMBER",
        float: "NUMBER",
        bool: "BOOLEAN",
        str: "VARCHAR",
    }
    multitype = "VARIANT"

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
        self._max_file_size = max_file_size
        self._table_path = table_path
        self._stage = stage
        self._stage_path = stage_path
        self._create_tables_on = create_tables_on
        self._populate_tables_on = populate_tables_on
        self._clear_stage_on = clear_stage_on
        self._allow_varying_value_types = allow_varying_value_types
        self._predefined_column_types = predefined_column_types or {}
        self._ignore_unexpected_fields = ignore_unexpected_fields
        self._instance_ms = int(time.time() * 1000)

        exporter_events = typing.get_args(ExporterEvent)
        for attr in ("create_tables_on", "populate_tables_on", "clear_stage_on"):
            val = getattr(self, "_" + attr)
            if val not in exporter_events:
                raise ValueError(
                    f"unexpected value ({val!r}) for {attr!r}, expected one of {exporter_events!r}"
                )

        if any(not v for v in self._predefined_column_types.values()):
            raise ValueError("values of 'predefined_column_types' can't be empty")

        if not stage.startswith("@") or "/" in stage:
            raise ValueError('"stage" must begin with @ and have no path elements')

        self.conn = self.create_connection(
            user=user,
            password=password,
            account=account,
            **(connection_kwargs or {}),
        )
        self._table_buffers: Dict = {}  # {table_path: tmp_buffer_file}
        self._exported_fpaths: Dict = {}  # {table_path: [exported_fpath_in_stage, ...]}
        self._recorded_coltypes: Dict = {}  # {table_path: {field: set({cls, ...})}}

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_connection(self, **connection_kwargs):
        return snowflake.connector.connect(**connection_kwargs)

    def close(self) -> None:
        self.conn.close()

    def export_item(self, item: Any, **extra_params) -> str:
        item_dict: Dict = ItemAdapter(item).asdict()

        table_path = self.table_for_item(item_dict, **extra_params)
        if table_path not in self._table_buffers:
            logger.info("Creating buffer for %r", table_path)
            self._table_buffers[table_path] = NamedTemporaryFile("wb")

        line = json.dumps(item_dict) + "\n"
        self._table_buffers[table_path].write(line.encode("utf8"))
        if self._should_flush_buffer_for(table_path):
            self.flush_table_buffer(table_path)

        self.record_field_types(table_path, item_dict)

        return table_path

    def _should_flush_buffer_for(self, table_path: str) -> bool:
        return self._table_buffers[table_path].tell() >= self._max_file_size

    def record_field_types(self, table_path: str, item_dict: Dict) -> None:
        recorded = self._recorded_coltypes.setdefault(table_path, {})
        for k, v in item_dict.items():
            if v is not None:
                recorded.setdefault(k, set()).add(self.typemap[type(v)])

    def table_for_item(self, item_dict: Dict, **extra_params) -> str:
        return self._table_path.format(**extra_params, item=item_dict)

    def fpath_for_table(self, table_path: str, batch_n: int) -> str:
        return self._stage_path.format(
            table_path=table_path,
            instance_ms=self._instance_ms,
            batch_n=batch_n,
        )

    def flush_table_buffer(self, table_path: str) -> None:
        # HACK: there is no straightforward way to provide a filename via PUT
        # statement, so as a workaround we symlink our file into a temporary
        # directory with the desired filename.
        tmp_file = self._table_buffers[table_path]
        tmp_file.flush()
        batch_n = len(self._exported_fpaths.setdefault(table_path, [])) + 1
        with TemporaryDirectory() as tempdir:
            fpath = self.fpath_for_table(table_path, batch_n)
            logger.info(
                "Flushing buffer of %r to %r (batch %d)", table_path, fpath, batch_n
            )
            prefix, fname = fpath.rsplit("/", 1)
            symlink_path = os.path.join(tempdir, fname)
            os.symlink(tmp_file.name, symlink_path)
            fpath = self._put_file(symlink_path, self._stage, prefix)
        tmp_file.close()
        self._table_buffers.pop(table_path)
        self._exported_fpaths[table_path].append(fpath)

        if self._create_tables_on == "flush":
            self.create_table(table_path)
        if self._populate_tables_on == "flush":
            self.populate_table(table_path)
        if self._clear_stage_on == "flush":
            self.clear_stage([fpath])

    def _put_file(self, fpath: str, stage: str, prefix: str) -> str:
        """Uploads a file to stage and returns resulted fpath in stage.
        Due to compression the resulting filename in stage can have an extra prefix (e.g. ".gz").
        """
        cursor = self.conn.cursor().execute(f"PUT 'file://{fpath}' '{stage}/{prefix}'")
        return prefix + "/" + cursor.fetchone()[1]

    def flush_all_table_buffers(self) -> None:
        for table_path in list(self._table_buffers):
            self.flush_table_buffer(table_path)

    def get_column_types(self, table_path: str) -> Dict[str, str]:
        columns = self._predefined_column_types.get(table_path, {})
        if columns and self._ignore_unexpected_fields:
            return columns

        recorded_coltypes = self._recorded_coltypes.get(table_path, {})
        for colname, coltypes in recorded_coltypes.items():
            if colname in columns:
                continue
            if len(coltypes) == 1:
                coltype = next(iter(coltypes))
            else:
                if self._allow_varying_value_types:
                    coltype = self.multitype
                else:
                    logger.error(
                        "Table %r, field %r: skipping. Multiple coltypes encountered on values: %r.",
                        table_path,
                        colname,
                        coltypes,
                    )
                    continue
            columns[colname] = coltype

        if not columns:
            raise SnowflakeStageExporterError(
                f"no valid columns are found for table {table_path!r}, recorded columns: {recorded_coltypes!r}",
            )

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
        logger.info("Populating table %r", table_path)
        all_fpaths = self._exported_fpaths[table_path]
        coltypes = self.get_column_types(table_path)
        cols = ", ".join(coltypes)
        json_select = ", ".join(f"$1:{field}" for field in coltypes)
        for fpaths in chunk(all_fpaths, 1000):
            fpaths = ", ".join(f"'{fpath}'" for fpath in fpaths)
            self.conn.cursor().execute(
                f"""
                COPY INTO {table_path} ({cols})
                    FROM (SELECT {json_select} FROM {self._stage})
                    FILE_FORMAT = (TYPE = JSON)
                    FILES = ({fpaths})
                """
            )

    def create_all_tables(self) -> None:
        for table_path in self._exported_fpaths:
            self.create_table(table_path)

    def populate_all_tables(self) -> None:
        for table_path in self._exported_fpaths:
            self.populate_table(table_path)

    def clear_stage(self, fpaths: Iterable[str] = None) -> None:
        if fpaths is None:
            fpaths = [
                fpath for fpaths in self._exported_fpaths.values() for fpath in fpaths
            ]
            count_msg = f"{len(fpaths)} (all)"
        else:
            fpaths = list(fpaths or [])
            count_msg = str(len(fpaths))
        logger.info("Removing %s staged files", count_msg)
        if fpaths:
            self.conn.cursor().executemany(
                "REMOVE %s", [(f"{self._stage}/{fpath}",) for fpath in fpaths]
            )

    def finish_export(self) -> None:
        self.flush_all_table_buffers()
        if self._create_tables_on == "finish":
            self.create_all_tables()
        if self._populate_tables_on == "finish":
            self.populate_all_tables()
        if self._clear_stage_on == "finish":
            self.clear_stage()
