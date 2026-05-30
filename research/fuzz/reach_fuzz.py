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

ORDER BY is part of the comparison: MS SQL's plan keeps the ORDER BY Sort on
top, and we keep it too. The CLI propagates the query's ORDER BY as a required
sort property, so the exhaustive search generates the matching Sort enforcer on
the root group and the ordered plan is genuinely reachable (or not — which is
exactly the divergence we want to surface).

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
    Aggregate,
    DIALECTS,
    QueryGenerator,
    SelectQuery,
    load_schema,
    render_query,
)


def _quote_alias(alias: str) -> str:
    return '"' + alias.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _render_attr(a) -> str:
    return f"(attr {a.table} {a.column})"


def _render_agg(agg: Aggregate) -> str:
    if agg.func == "COUNT" and agg.arg is None:
        return "(COUNT *)"
    return f"({agg.func} {_render_attr(agg.arg)})"


def _split_sexpr_args(inner: str) -> list[str]:
    """Split the body of an s-expr into top-level atoms/groups, respecting
    nested parens and double-quoted strings."""
    out: list[str] = []
    i, n = 0, len(inner)
    while i < n:
        while i < n and inner[i].isspace():
            i += 1
        if i >= n:
            break
        if inner[i] == "(":
            depth, j = 0, i
            while j < n:
                c = inner[j]
                if c == '"':
                    j += 1
                    while j < n and inner[j] != '"':
                        j += 2 if inner[j] == "\\" else 1
                    j += 1
                    continue
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        j += 1
                        break
                j += 1
            out.append(inner[i:j])
            i = j
        else:
            j = i
            while j < n and not inner[j].isspace():
                j += 1
            out.append(inner[i:j])
            i = j
    return out


def _top_node(sexpr: str) -> tuple[str, list[str]]:
    s = sexpr.strip()
    args = _split_sexpr_args(s[1:-1])
    return args[0], args[1:]


def _make_projection(plan_sexpr: str, query: SelectQuery) -> str:
    """Wrap `plan_sexpr` in the PhysicalProjection our visitor emits.

    For GROUP BY queries this sits above the HashAggregate the converter
    produced: the C++ visitor replaces each aggregate target with a synthetic
    attribute ``__aggN`` (empty table, counter in target order) and keeps the
    HashAggregate's aggregate expressions in its ``aggs`` list."""
    exprs = []
    agg_counter = 0
    for t in query.targets:
        if isinstance(t.expr, Aggregate):
            # Synthetic aggregate-output attribute: empty table, serialized "-".
            exprs.append(f"(attr - __agg{agg_counter})")
            agg_counter += 1
        else:  # plain column target
            a = t.expr
            exprs.append(f"(attr {a.table} {a.column})")
    exprs_str = " ".join(exprs)

    # The visitor clears the alias list when no target is aliased; mirror that
    # so the serialized shape matches (with vs without the (aliases ...) form).
    if any(t.alias for t in query.targets):
        aliases = " ".join(_quote_alias(t.alias) if t.alias else "-" for t in query.targets)
        return f"(PhysicalProjection (exprs {exprs_str}) (aliases {aliases}) {plan_sexpr})"
    return f"(PhysicalProjection (exprs {exprs_str}) {plan_sexpr})"


def _rebuild_aggregate(plan_sexpr: str, query: SelectQuery) -> str:
    """Replace a converted HashAggregate's group_by/aggs with query-derived
    ones, keeping only its converted source subtree.

    MS SQL augments aggregation with hidden helper aggregates (e.g. a COUNT_BIG
    alongside every SUM, for the empty-set NULL semantics) and orders them by
    its own internal column ids, so its agg list never matches ours. The
    aggregates are pure query semantics both engines share, so we emit them from
    the query in target order — the same order the C++ visitor's
    CollectAggregates uses — and reuse only the real input subtree from MS."""
    head, args = _top_node(plan_sexpr)
    if head != "HashAggregate" or len(args) != 3:
        return plan_sexpr  # unexpected shape; leave as-is (best effort)
    child = args[2]

    group_by = " ".join(_render_attr(a) for a in query.group_by)
    aggs = " ".join(
        _render_agg(t.expr) for t in query.targets if isinstance(t.expr, Aggregate)
    )
    return f"(HashAggregate (group_by {group_by}) (aggs {aggs}) {child})"


def _wrap_projection(plan_sexpr: str, query: SelectQuery) -> str:
    """Insert the projection node the optimizer's search produces, keeping any
    top-of-plan ORDER BY Sort.

    MS SQL places the ORDER BY Sort above the projection. Our optimizer reaches
    that exact shape: with the query's ORDER BY propagated as a required sort
    property (see IsPlanReachable), the search puts a Sort *enforcer* on the
    root projection group. So we lift the converter's Sort off the top, nest the
    synthesized projection beneath it, and put the Sort back — yielding
    ``(Sort (keys ...) (PhysicalProjection ... <child>))``. The Sort keys pass
    through verbatim from the converter; they match the enforcer's keys, which
    the CLI derives from the same ORDER BY clause.

    For non-aggregate column targets the converter emits no projection of its
    own (MS SQL just selects the columns), so the projection is always
    synthesized here regardless of the Sort."""
    head, args = _top_node(plan_sexpr)
    sort_keys = None
    if head == "Sort" and len(args) == 2:
        sort_keys, plan_sexpr = args[0], args[1]
    if query.group_by:
        plan_sexpr = _rebuild_aggregate(plan_sexpr, query)
    projected = _make_projection(plan_sexpr, query)
    if sort_keys is not None:
        return f"(Sort {sort_keys} {projected})"
    return projected


def _run_reach_check(
    cli: str, data_dir: str, sql: str, plan_sexpr: str
) -> subprocess.CompletedProcess:
    with tempfile.NamedTemporaryFile("w", suffix=".plan", delete=False) as f:
        f.write(plan_sexpr)
        plan_path = f.name
    try:
        # --data-dir lets the Sort enforcer validate ORDER BY keys against the
        # group schema, matching how the real optimizer places enforcers.
        return subprocess.run(
            [cli, "--check-reachable", plan_path, "--data-dir", data_dir],
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
            proc = _run_reach_check(args.cli, args.data_dir, ours_sql, target_plan)
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
