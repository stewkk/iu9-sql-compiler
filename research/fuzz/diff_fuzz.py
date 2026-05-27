"""
Differential fuzzer: generate a random SQL query, run it through our compiler
and MS SQL Server, stop on the first observable divergence.

Compares result rows as multisets of tab-separated strings. Column names are
ignored (the two engines disagree on naming conventions), but column count
must match.

Usage:
    python -m research.fuzz.diff_fuzz \\
        --cli build/bin/sql \\
        --data-dir test/static/executor/test_data \\
        [--start-seed 0]
"""
from __future__ import annotations

import argparse
import random
import subprocess
import sys
from itertools import count
from pathlib import Path

from research.fuzz.mssql_runner import MsSqlRunner, RunResult
from research.query_generator import (
    DIALECTS,
    QueryGenerator,
    SelectQuery,
    load_schema,
    render_query,
)


def _value_to_canonical(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    return str(v)


def _mssql_canonical_rows(res: RunResult) -> list[str]:
    return ["\t".join(_value_to_canonical(v) for v in row) for row in res.rows]


def _ours_canonical_rows(stdout: str) -> tuple[int, list[str]]:
    """Parse the CLI's output. Returns (column_count, row_strings)."""
    lines = stdout.splitlines()
    if not lines:
        return 0, []
    header = lines[0]
    col_count = len(header.split("\t"))
    return col_count, lines[1:]


def _run_ours(cli: str, data_dir: str, query: str, jit: bool) -> subprocess.CompletedProcess:
    cmd = [cli, "--data-dir", data_dir]
    if jit:
        cmd.append("--jit")
    return subprocess.run(
        cmd,
        input=query,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _compare(
    query: SelectQuery,
    ours_proc: subprocess.CompletedProcess,
    theirs: RunResult,
) -> str | None:
    """Returns a diagnostic string on divergence, None on match."""
    if ours_proc.returncode != 0:
        return f"our CLI exited {ours_proc.returncode}\nstderr: {ours_proc.stderr.strip()}"
    if theirs.error is not None:
        return f"MS SQL rejected the query: {theirs.error}"

    our_cols, our_rows = _ours_canonical_rows(ours_proc.stdout)
    their_rows = _mssql_canonical_rows(theirs)
    their_cols = len(theirs.columns)

    if our_cols != their_cols:
        return f"column count: ours={our_cols} theirs={their_cols}"

    # Generator does not emit ORDER BY, so always compare as multisets.
    if sorted(our_rows) != sorted(their_rows):
        return "row contents differ"

    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cli", required=True, help="Path to our sql binary")
    ap.add_argument("--data-dir", required=True, help="CSV table directory")
    ap.add_argument("--start-seed", type=int, default=0)
    ap.add_argument("--jit", action="store_true", help="Run our CLI with the JIT executor")
    ap.add_argument("--mssql-host", default="localhost")
    ap.add_argument("--mssql-port", type=int, default=1433)
    ap.add_argument("--mssql-user", default="sa")
    ap.add_argument("--mssql-password", default="Password123!")
    ap.add_argument("--mssql-database", default="fuzz")
    args = ap.parse_args()

    schema = load_schema(Path(args.data_dir))

    mssql = MsSqlRunner(
        host=args.mssql_host,
        port=args.mssql_port,
        user=args.mssql_user,
        password=args.mssql_password,
        database=args.mssql_database,
    )
    print("Setting up MS SQL schema...", flush=True)
    mssql.setup_schema(args.data_dir)
    print("Schema ready. Starting fuzz loop.\n", flush=True)

    pg = DIALECTS["pg"]
    ms = DIALECTS["mssql"]

    for seed in count(args.start_seed):
        rng = random.Random(seed)
        query = QueryGenerator(schema, rng).generate()
        ours_sql = render_query(query, pg) + ";"
        theirs_sql = render_query(query, ms) + ";"

        try:
            ours_proc = _run_ours(args.cli, args.data_dir, ours_sql, args.jit)
        except subprocess.TimeoutExpired:
            print(f"DIVERGENCE seed={seed}: our CLI timed out\n--- query (ours):\n{ours_sql}")
            sys.exit(1)

        theirs = mssql.run(theirs_sql)

        diag = _compare(query, ours_proc, theirs)
        if diag is not None:
            print(f"DIVERGENCE seed={seed}: {diag}")
            print(f"\n--- query (ours):\n{ours_sql}")
            print(f"\n--- query (mssql):\n{theirs_sql}")
            print(f"\n--- ours stdout:\n{ours_proc.stdout}", end="")
            if ours_proc.stderr:
                print(f"\n--- ours stderr:\n{ours_proc.stderr}", end="")
            print(f"\n--- mssql columns: {theirs.columns}")
            print(f"--- mssql rows ({len(theirs.rows)}):")
            for r in theirs.rows[:50]:
                print(r)
            if len(theirs.rows) > 50:
                print(f"... ({len(theirs.rows) - 50} more)")
            sys.exit(1)

        if seed % 25 == 0:
            print(f"seed={seed} ok", flush=True)


if __name__ == "__main__":
    main()
