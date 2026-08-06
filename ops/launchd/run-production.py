#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


project_dir = Path(
    os.environ.get("MARKETCOW_PROJECT_DIR", "/Volumes/T9/projects/marketcow")
)
env_file = Path(os.environ["MARKETCOW_ENV_FILE"])
load_dotenv(env_file, override=False)
profile = os.environ.get("MARKETCOW_PROFILE", "production")
os.chdir(project_dir)
os.execv(
    sys.executable,
    [
        sys.executable,
        "-m",
        "marketcow",
        "--profile",
        profile,
        "start",
        "--host",
        "127.0.0.1",
        "--port",
        "8790",
    ],
)
