# Snowflake Stage Exporter
Snowflake database loading utility with [**Scrapy**](https://scrapy.org/) integration.  
Meant for streaming ingestion of JSON serializable objects into Snowflake stages and tables.  

## Installation
```bash
> pip install git+https://github.com/hermit-crab/snowflake-stage-exporter.git
```

## Basic example

```python
from snowflake_stage_exporter import SnowflakeStageExporter

with SnowflakeStageExporter(
    user="...",
    password="...",
    account="...",
    table_path="MY_DATABASE.PUBLIC.{item_type_name}",
) as exporter:
    exporter.export_item({"name": "Jack", "salary": 100}, item_type_name="employee")
    exporter.export_item({"name": "Sal", "salary": 90, "extra_info": {"age": 20}}, item_type_name="employee")
    exporter.export_item({"title": "Steel Grill", "price": 5.5}, item_type_name="product")
    exporter.finish_export()  # flushes all stage buffers, creates tables and populates them with data inside stages
```

After you call `finish_export()` 2 tables will be created: `EMPLOYEE` (2 rows, 3 columns) and `PRODUCT` (1 row, 2 columns) located inside database `MY_DATABASE` and database schema `PUBLIC` (Snowflake [default database schema](https://docs.snowflake.com/en/sql-reference/sql/create-database.html#general-usage-notes)).

[Same thing achieved via Scrapy integration](./docs/scrapy_basic_example.md).

## How this works

For each object that you feed into the exporter it will write it into a local buffer (temporary JSON file). Once a configurable maximum buffer size is reached the file is uploaded to [Snowflake internal stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html) via [PUT statement](https://docs.snowflake.com/en/sql-reference/sql/put.html). Upon the end of the execution exporter will create all specified tables then instruct Snowflake to populate each table from every staged JSON file via [`COPY INTO <table>` statements](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html).

- If you output to multiple tables then a buffer is maintained for each.
- Alternatively you can create / populate tables as soon as the buffers are flushed via `*_on` parameters described below.
- `*_on` parameters also allow you to disable any table creation / population and just deal with the stages yourself.
- For table creation the exporter will try to figure out column types dynamically during execution, otherwise you can pass them explicitly via parameter.

## Why "Stages"?

The use of local buffers and stages opposed to typical SQL `INSERT` statements is motivated largely by Snowflake performance implications and their billing model (see see [this topic](https://community.snowflake.com/s/question/0D50Z00008JpBymSAF/implications-of-multiple-insert-statements-vs-copy-into) on Snowflake official community forum).

An illustrative example can be a long running Scrapy / ScrapyCloud job that constantly outputs data. If the job was to keep the connection constantly executing the INSERTs - Snowflake would also keep the warehouse running / consuming the credits for the entire duration of the job.

Secondary consideration was for allowing the user to be able to work with purely just the stages like one would work with S3 or similar blob file storage. This covers cases when user would needs their own sophisticated table management approach and simply wants a convenient place to store raw data.

**IMPORTANT NOTE**: it won't make much sense to use this library if you're already working with S3 or similar storages (including just local machine) where your data is one of the [Snowflake supported file formats](https://docs.snowflake.com/en/user-guide/data-load-prepare.html#supported-file-formats). Snowflake [has built-in support of ingesting several 3rd party blob storages](https://docs.snowflake.com/en/user-guide/data-load-bulk.html) and for local files you can upload them via `PUT` statements.

## Configuration

All of the configurations are done via arguments to main exporter class `SnowflakeStageExporter`.

- `user/password/account` - Snowflake account credentials, passed as is to [`snowflake.connector.connect`](https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect). 
- `connection_kwargs` - any additional parameters to `snowflake.connector.connect`.
- `table_path` - table path to use.
    - If you specify database / database schema in `connection_kwargs` you won't need to specify them in the table path.
    - The path can include template variables which are expanded when you feed an item to exporter. By default only `item` variable is passed (e.g. `"MY_DB.PUBLIC.TABLE_{item[entity_type]}"` here it's assumed all of your items have "entity_type" field).
    - Any additional variables you can pass yourself as keyword arguments when calling `exporter.export_item()`.
    - Additionally in Scrapy integration the following fields are passed:
        - `spider` - spider instance.
        - `item_type_name` - `type(item).__name__`. In the basic example above you passed this explicitly yourself.
- `stage` - [which internal stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage.html#listing-staged-data-files) to use. By default user stage (`"@~"`) is used.
- `stage_path` - naming for the files being uploaded to the stage.
    - By default it's `"{table_path}/{instance_ms}_{batch_n}.jl"` where `table_path` is `table_path` with all variables resolved, `instance_ms` epoch milliseconds when exporter was instantiated and `batch_n` being sequential number of the buffer.
    - In Scrapy integration by default this is `"{table_path}/{job}/{instance_ms}_{batch_n}.jl"` where `job` is the key of the ScrapyCloud job or `"local"` if spider ran locally.
- `max_file_size` - maximum buffer size in bytes. 1GiB by default.
- `predefined_column_types` - dictionary of `table_path` to Snowflake columns types for table creation.
    - e.g. `{"MY_DB.PUBLIC.PRODUCT": {"title": "STRING", "price": "NUMBER"}, "MY_DB.PUBLIC.EMPLOYEE": {"name": "STRING", "salary": "NUMBER", "extra_info": "OBJECT"}}`.
- `ignore_unexpected_fields` - ignore fields not passed in `predefined_column_types` during table creation / population.
    - `True` by default but only takes effect when table does have predefined column types.
    - The data is still exported in full to the staged files.
- `allow_varying_value_types` - if `False` during table creation / population skip columns that had multiple value types.
    - `False` by default. Error is logged during table create/populate if such columns were encountered.
    - If `True` then `VARIANT` type is assigned to such column.
    - Takes effect only when there is a need for exporter to figure out the column type.
    - The data is still exported in full to the staged files.
- `create_tables_on` - one of "finish/flush/never". "finish" by default. "flush" is for each time a file is staged.
    - If you are not providing `predefined_column_types`, note that using "flush" will constraint your tables to only be populated with columns encountered at the point when first "flush" happened.
- `populate_tables_on` - same as above.
- `clear_stage_on` - same as above but "never" is default. Each file is removed from stage individually when enabled.

## Configuration (Scrapy)

All of the exporter instance parameters are exposed as Scrapy settings like `SNOWFLAKE_<UPPERCASE_PARAMETER_NAME>` (e.g. `SNOWFLAKE_MAX_FILE_SIZE`).

Once a Scrapy job ends, all remaining buffers are flushed. If the job outcome is not "finished" (something went wrong) then no table creation / table population / stage clear takes place.

## Development

```bash
> pip install black mypy pylint isort pytest # instal dev dependencies
> python ./run_checks.py # run lints and tests
```
