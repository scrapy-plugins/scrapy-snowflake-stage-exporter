#!/usr/bin/env python
import subprocess
import sys

code = 0
for cmd in [
    "pylint snowflake_stage_exporter tests",
    "mypy snowflake_stage_exporter tests",
    "black snowflake_stage_exporter tests",
    "pytest tests",
]:
    print(f">> CHECK: {cmd}")
    try:
        subprocess.run(cmd.split(), check=True)
    except subprocess.CalledProcessError:
        print(f">> FAIL: {cmd}")
        code = 1
sys.exit(code)
