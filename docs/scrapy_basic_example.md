## Basic example of use with Scrapy

NOTE: This example uses dataclasses for items, however it will also work just fine with `scrapy.Item`'s or `dict`s.

```python
# your Scrapy `settings.py` file
...
ITEM_PIPELINES = {
    "snowflake_stage_exporter.scrapy_pipeline.SnowflakePipeline": 100
}
SNOWFLAKE_USER = "..."
SNOWFLAKE_PASSWORD = "..."
SNOWFLAKE_ACCOUNT = "..."
SNOWFLAKE_TABLE_PATH = "DEMO_DB.PUBLIC.{item_type_name}"
...
```

```python
# your Scrapy spider
import scrapy
from dataclasses import dataclass


@dataclass
class Employee:
    name: str
    salary: int
    extra_info: dict = None


@dataclass
class Product:
    title: str
    price: float


class MySpider(scrapy.Spider):
    name = "test_spider"
    start_urls = ["https://books.toscrape.com/"]

    def parse(self, response, **kwargs):
        yield Employee(name="Jack", salary=100)
        yield Employee(name="Sal", salary=90, extra_info={"age": 20})
        yield Product(name="Steel Grill", price=5.5)
```
