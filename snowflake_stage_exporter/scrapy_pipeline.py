import inspect
import os

from . import SnowflakeStageExporter


class SnowflakePipeline:

    stage_exporter_class = SnowflakeStageExporter
    setting_getters = {
        "connection_kwargs": "getdict",
        "max_file_size": "getint",
        "predefined_column_types": "getdict",
        "ignore_unexpected_fields": "getbool",
        "allow_varying_value_types": "getbool",
    }

    def __init__(self, settings):
        job = os.environ.get("SHUB_JOBKEY", "local")
        kwargs = self._turn_settings_to_kwargs(settings)
        kwargs.setdefault(
            "stage_path", "{table_path}/" + job + "/{instance_ms}_{batch_n}.jl"
        )
        self.stage_exporter = self.stage_exporter_class(**kwargs)

    def _turn_settings_to_kwargs(self, settings):
        stage_exporter_kwargs = {}
        for param_name, param_spec in inspect.signature(
            self.stage_exporter_class
        ).parameters.items():
            settings_key = "SNOWFLAKE_" + param_name.upper()
            if settings_key not in settings:
                if param_spec.default == param_spec.empty:
                    raise ValueError(f"Setting {settings_key!r} is required")
                continue
            stage_exporter_kwargs[param_name] = getattr(
                settings, self.setting_getters.get(param_name, "get")
            )(settings_key)
        return stage_exporter_kwargs

    @classmethod
    def from_crawler(cls, crawler):
        from scrapy import signals  # type: ignore # pylint: disable=import-error

        instance = cls(crawler.settings)
        crawler.signals.connect(instance.on_spider_close, signal=signals.spider_closed)
        return instance

    def process_item(self, item, spider):
        self.stage_exporter.export_item(
            item, spider=spider, item_type_name=type(item).__name__
        )
        return item

    def on_spider_close(self, spider, reason):
        # pylint: disable=unused-argument
        self.stage_exporter.flush_all_table_buffers()
        if reason == "finished":
            self.stage_exporter.finish_export()
        self.stage_exporter.close()
