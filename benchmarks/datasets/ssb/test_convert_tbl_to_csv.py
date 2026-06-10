from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


SSB_DIR = Path(__file__).resolve().parent
CONVERTER = SSB_DIR / "convert_tbl_to_csv.py"
SCHEMA = SSB_DIR / "schema.json"
FIXTURE_TBL = SSB_DIR / "fixtures" / "tbl"
EXPECTED_CSV = SSB_DIR / "fixtures" / "expected_csv"


def run_converter(input_dir: Path, output_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(CONVERTER),
            "--schema",
            str(SCHEMA),
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_converts_all_ssb_tables(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"

    result = run_converter(FIXTURE_TBL, output_dir)

    assert result.returncode == 0, result.stderr
    expected_files = sorted(path.name for path in EXPECTED_CSV.glob("*.csv"))
    actual_files = sorted(path.name for path in output_dir.glob("*.csv"))
    assert actual_files == expected_files
    for filename in expected_files:
        assert (output_dir / filename).read_text() == (EXPECTED_CSV / filename).read_text()


def test_rejects_malformed_row_width(tmp_path: Path) -> None:
    input_dir = tmp_path / "tbl"
    shutil.copytree(FIXTURE_TBL, input_dir)
    with (input_dir / "lineorder.tbl").open("a", encoding="utf-8") as f:
        f.write("3|too-few-fields|\n")

    result = run_converter(input_dir, tmp_path / "out")

    assert result.returncode != 0
    assert "expected 17 fields for lineorder" in result.stderr


def test_rejects_missing_input_file(tmp_path: Path) -> None:
    input_dir = tmp_path / "tbl"
    shutil.copytree(FIXTURE_TBL, input_dir)
    (input_dir / "supplier.tbl").unlink()

    result = run_converter(input_dir, tmp_path / "out")

    assert result.returncode != 0
    assert "missing input file" in result.stderr
    assert "supplier.tbl" in result.stderr


def test_rejects_invalid_integer(tmp_path: Path) -> None:
    input_dir = tmp_path / "tbl"
    shutil.copytree(FIXTURE_TBL, input_dir)
    (input_dir / "date.tbl").write_text(
        "not-an-int|January 1, 1994|Saturday|January|1994|199401|Jan1994|6|1|1|1|1|Winter|0|0|1|0|\n",
        encoding="utf-8",
    )

    result = run_converter(input_dir, tmp_path / "out")

    assert result.returncode != 0
    assert "d_datekey expects int" in result.stderr
