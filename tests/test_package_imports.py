"""Regression tests for package import behavior."""

import subprocess
import sys
from pathlib import Path


def test_reader_imports_do_not_eagerly_import_unrelated_domains():
    repo_root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        "-c",
        (
            "import importlib, sys; "
            "importlib.import_module('data_fetcher.readers.kabu_tick'); "
            "importlib.import_module('data_fetcher.readers.sbi'); "
            "import data_fetcher; "
            "assert 'data_fetcher.domains' not in sys.modules; "
            "assert data_fetcher.readers.__name__ == 'data_fetcher.readers'"
        ),
    ]
    result = subprocess.run(
        command,
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
