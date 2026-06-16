from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


SSB_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SSB_DIR.parents[2]
SQL_CLI = PROJECT_DIR / "build" / "bin" / "sql"
QUERY_DIR = SSB_DIR / "queries"
EXPECTED_CSV = SSB_DIR / "fixtures" / "expected_csv"

SSB_QUERIES = tuple(sorted(QUERY_DIR.glob("*.sql")))


@pytest.mark.parametrize("query_path", SSB_QUERIES, ids=lambda p: p.stem)
def test_ssb_query_executes_against_expected_csv(query_path: Path) -> None:
    assert SQL_CLI.exists(), f"missing SQL CLI: {SQL_CLI}; build the sql target first"

    result = subprocess.run(
        [str(SQL_CLI), "--data-dir", str(EXPECTED_CSV)],
        input=query_path.read_text(encoding="utf-8"),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"{query_path.name} failed with exit code {result.returncode}\n"
        f"stderr:\n{result.stderr}\n"
        f"stdout:\n{result.stdout}"
    )
