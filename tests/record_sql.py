# pylint: disable=no-member
from pprint import pprint

from tests import make_test_exporter, mock_calls_get_sql


def main():
    with make_test_exporter("your_table_path") as exporter:

        # <your code above>
        pprint(mock_calls_get_sql(exporter.conn.cursor().execute.mock_calls))


if __name__ == "__main__":
    main()
