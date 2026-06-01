"""
Collect (plan cost, actual execution time) pairs over random queries.

Generates random queries with the shared query generator, runs each through our
compiler with --stats, and records the optimizer's chosen-plan cost alongside
the measured execution wall time (microseconds, taken inside the process so it
excludes interpreter/process startup). To damp timing noise each query is run
several times and the median execution time is kept.

The resulting CSV (seed, plan_cost, exec_us, rows, query) feeds
research/cost-on-random.ipynb, which checks whether cost tracks real time.

Usage:
    python -m research.fuzz.cost_stats \\
        --cli build-release/bin/sql \\
        --data-dir test/static/executor/test_data \\
        --count 500 \\
        --out research/cost-stats.csv
"""
from __future__ import annotations

import argparse
import csv
import random
import re
import statistics
import subprocess
import sys
from itertools import count
from pathlib import Path

from research.query_generator import DIALECTS, QueryGenerator, load_schema, render_query

_STATS_RE = re.compile(r"STATS plan_cost=(-?\d+) exec_us=(-?\d+) rows=(\d+)")


def _run_ours(cli: str, data_dir: str, query: str, jit: bool) -> subprocess.CompletedProcess:
    cmd = [cli, "--data-dir", data_dir, "--stats"]
    if jit:
        cmd.append("--jit")
    return subprocess.run(
        cmd,
        input=query,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _parse_stats(stderr: str) -> tuple[int, int, int] | None:
    """Return (plan_cost, exec_us, rows) from the CLI's STATS line, or None."""
    m = _STATS_RE.search(stderr)
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cli", required=True, help="Path to our sql binary")
    ap.add_argument("--data-dir", required=True, help="CSV table directory")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--start-seed", type=int, default=0)
    ap.add_argument("--count", type=int, default=500, help="How many valid samples to collect")
    ap.add_argument("--repeats", type=int, default=5,
                    help="Runs per query; the median exec time is kept")
    ap.add_argument("--jit", action="store_true", help="Run our CLI with the JIT executor")
    args = ap.parse_args()

    schema = load_schema(Path(args.data_dir))
    pg = DIALECTS["pg"]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    collected = 0
    attempted = 0
    skipped = 0
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seed", "plan_cost", "exec_us", "rows", "query"])

        for seed in count(args.start_seed):
            if collected >= args.count:
                break
            attempted += 1
            rng = random.Random(seed)
            query = QueryGenerator(schema, rng).generate()
            sql = render_query(query, pg) + ";"

            cost: int | None = None
            rows: int | None = None
            samples: list[int] = []
            failed = False
            for _ in range(args.repeats):
                try:
                    proc = _run_ours(args.cli, args.data_dir, sql, args.jit)
                except subprocess.TimeoutExpired:
                    failed = True
                    break
                if proc.returncode != 0:
                    failed = True
                    break
                parsed = _parse_stats(proc.stderr)
                if parsed is None:
                    failed = True
                    break
                cost, exec_us, rows = parsed
                samples.append(exec_us)

            if failed or not samples:
                skipped += 1
                continue

            exec_us = int(statistics.median(samples))
            writer.writerow([seed, cost, exec_us, rows, sql])
            f.flush()
            collected += 1
            if collected % 25 == 0:
                print(f"collected={collected} attempted={attempted} skipped={skipped}",
                      flush=True)

    print(f"done: collected={collected} attempted={attempted} skipped={skipped} -> {out_path}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
