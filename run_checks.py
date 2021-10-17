#!/usr/bin/env python
import subprocess
import sys

code = 0
for cmd in [
    "pylint snowflake_stage_exporter tests",
    "mypy snowflake_stage_exporter tests",
    "black snowflake_stage_exporter tests --check",
    "isort snowflake_stage_exporter tests --check",
    "pytest tests",
]:
    print(">" * 10 + f" CHECK: {cmd}")
    try:
        subprocess.run(cmd.split(), check=True)
    except subprocess.CalledProcessError:
        print("x" * 10 + f" FAIL: {cmd}")
        code = 1
sys.exit(code)
