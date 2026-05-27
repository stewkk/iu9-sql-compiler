"""
Reachability fuzzer: generate a random SQL query, ask MS SQL Server for its
execution plan, convert that plan to the project's serialized format, then
invoke our CLI in ``--check-reachable`` mode to confirm that our exhaustive
search would have considered it. Stops at the first plan our optimizer cannot
reach.

The fuzzer only flags a divergence when the MS SQL plan converts cleanly but
our optimizer cannot reach it. Coverage gaps in the XML→s-expr converter
(unhandled physical operators or scalar shapes) are logged and skipped, since
they are converter limitations rather than optimizer bugs.

Usage:
    python -m research.fuzz.reach_fuzz \\
        --cli build/bin/sql \\
        --data-dir test/static/executor/test_data \\
        [--start-seed 0]
"""
from __future__ import annotations

import argparse
import random
import subprocess
import sys
import tempfile
from itertools import count
from pathlib import Path

from research.converter import convert as convert_plan
from research.fuzz.mssql_runner import MsSqlRunner
from research.query_generator import (
    DIALECTS,
    QueryGenerator,
    SelectQuery,
    load_schema,
    render_query,
)


def _wrap_projection(plan_sexpr: str, query: SelectQuery) -> str:
    """Our parser always emits a Projection at the top of a SELECT with
    targets. MS SQL Server's plans express projection implicitly via the
    scan/join OutputList rather than as a node, so we add the wrapper here so
    the shape matches what the optimizer's exhaustive search produces."""
    exprs = " ".join(f"(attr {a.table} {a.column})" for a in query.targets)
    return f"(PhysicalProjection (exprs {exprs}) {plan_sexpr})"


def _run_reach_check(cli: str, sql: str, plan_sexpr: str) -> subprocess.CompletedProcess:
    with tempfile.NamedTemporaryFile("w", suffix=".plan", delete=False) as f:
        f.write(plan_sexpr)
        plan_path = f.name
    try:
        return subprocess.run(
            [cli, "--check-reachable", plan_path],
            input=sql,
            capture_output=True,
            text=True,
            timeout=60,
        )
    finally:
        Path(plan_path).unlink(missing_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cli", required=True, help="Path to our sql binary")
    ap.add_argument("--data-dir", required=True, help="CSV table directory (shared with MS SQL)")
    ap.add_argument("--start-seed", type=int, default=0)
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
    print("Schema ready. Starting reachability fuzz loop.\n", flush=True)

    pg = DIALECTS["pg"]
    ms = DIALECTS["mssql"]

    skipped_convert = 0
    skipped_mssql = 0

    for seed in count(args.start_seed):
        rng = random.Random(seed)
        query = QueryGenerator(schema, rng).generate()
        ours_sql = render_query(query, pg) + ";"
        # OPTION (RECOMPILE) embeds literal values in the plan so the converter
        # sees Const nodes instead of @P parameter references.
        theirs_sql = render_query(query, ms) + " OPTION (RECOMPILE);"

        try:
            plan_xml = mssql.get_plan(theirs_sql)
        except Exception as e:
            skipped_mssql += 1
            if seed % 25 == 0:
                print(f"seed={seed} mssql refused: {e}", flush=True)
            continue

        try:
            converted = convert_plan(plan_xml)
        except NotImplementedError as e:
            skipped_convert += 1
            if seed % 25 == 0:
                print(f"seed={seed} converter skip: {e}", flush=True)
            continue
        except Exception as e:
            print(f"seed={seed} converter error: {e}\n--- query:\n{theirs_sql}")
            sys.exit(2)

        target_plan = _wrap_projection(converted, query)

        try:
            proc = _run_reach_check(args.cli, ours_sql, target_plan)
        except subprocess.TimeoutExpired:
            print(f"DIVERGENCE seed={seed}: reachability check timed out")
            print(f"\n--- query (ours):\n{ours_sql}")
            print(f"\n--- target plan:\n{target_plan}")
            sys.exit(1)

        if proc.returncode == 0:
            if seed % 25 == 0:
                print(
                    f"seed={seed} ok"
                    f" (skipped: {skipped_convert} convert, {skipped_mssql} mssql)",
                    flush=True,
                )
            continue

        # Non-zero: either parse/optimizer error, or NOT REACHABLE.
        print(f"DIVERGENCE seed={seed}: exit={proc.returncode}")
        print(f"\n--- query (ours):\n{ours_sql}")
        print(f"\n--- query (mssql):\n{theirs_sql}")
        print(f"\n--- mssql plan (converted):\n{target_plan}")
        if proc.stdout:
            print(f"\n--- stdout:\n{proc.stdout}", end="")
        if proc.stderr:
            print(f"\n--- stderr:\n{proc.stderr}", end="")
        sys.exit(1)


if __name__ == "__main__":
    main()
