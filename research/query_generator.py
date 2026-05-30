#!/usr/bin/env python3
"""
Random SQL query generator.

Builds queries in the subset that the C++ parser accepts — SELECT-Project-
Filter-Join plus table/column aliases, GROUP BY with SUM/COUNT aggregates,
IN/NOT IN, BETWEEN, string columns/literals and ORDER BY — then renders them in
pluggable SQL dialects.

Currently supported dialects:
  - PostgresSubsetDialect  (the subset the C++ ANTLR visitor accepts)
  - MsSqlDialect           (T-SQL with bracket quoting and schema prefix)

Usage:
    python query_generator.py \\
        --data-dir ../test/static/executor/test_data \\
        [--seed 42] [--count 5] [--dialect pg|mssql]
"""
from __future__ import annotations

import argparse
import csv
import re
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

@dataclass
class Column:
    name: str
    type: str  # "int" | "string"


@dataclass
class TableSchema:
    name: str
    columns: list[Column]
    sample_values: dict[str, list]  # col_name -> sampled actual values (typed)


@dataclass
class Schema:
    tables: dict[str, TableSchema]


def load_schema(csv_dir: Path, max_sample: int = 200) -> Schema:
    """
    Read all *.csv files from csv_dir.
    Files whose stem ends in _<digits> (e.g. departments_1000) are skipped —
    they share a schema with the base table and are only used for benchmarks.
    """
    tables: dict[str, TableSchema] = {}
    for path in sorted(csv_dir.glob("*.csv")):
        if re.search(r"_\d+$", path.stem):
            continue
        with path.open() as f:
            reader = csv.reader(f)
            header_row = next(reader)
            columns = []
            for h in header_row:
                col_name, col_type = h.strip().split(":")
                columns.append(Column(col_name.strip(), col_type.strip()))

            by_name = {c.name: c for c in columns}
            sample_values: dict[str, list] = {c.name: [] for c in columns}
            for i, row in enumerate(reader):
                if i >= max_sample:
                    break
                for col, val in zip(columns, row):
                    v = val.strip()
                    if v == "NULL":
                        continue
                    if by_name[col.name].type == "int":
                        try:
                            sample_values[col.name].append(int(v))
                        except ValueError:
                            pass
                    else:
                        sample_values[col.name].append(v)

        tables[path.stem] = TableSchema(path.stem, columns, sample_values)
    return Schema(tables)


# ---------------------------------------------------------------------------
# Query IR — only constructs that the C++ parser accepts
# ---------------------------------------------------------------------------

@dataclass
class Attr:
    table: str   # visible name (alias if the scan is aliased, else table name)
    column: str


@dataclass
class IntLit:
    value: int


@dataclass
class StrLit:
    value: str


@dataclass
class NullLit:
    pass


@dataclass
class BoolLit:
    value: bool


@dataclass
class BinaryExpr:
    # Comparison: "=", "!=", "<", ">", "<=", ">="
    # Logical:    "and", "or"
    # Arithmetic: "+", "-", "*"
    op: str
    lhs: "Expr"
    rhs: "Expr"


@dataclass
class UnaryExpr:
    op: str   # "not" | "uminus"
    child: "Expr"


@dataclass
class IsNullExpr:
    child: "Expr"
    negated: bool   # False -> IS NULL, True -> IS NOT NULL


@dataclass
class InExpr:
    attr: Attr
    values: list["Expr"]   # IntLit / StrLit, emitted sorted ascending
    negated: bool          # False -> IN, True -> NOT IN


@dataclass
class BetweenExpr:
    attr: Attr             # int columns only
    lo: "Expr"
    hi: "Expr"
    negated: bool          # False -> BETWEEN, True -> NOT BETWEEN


@dataclass
class Aggregate:
    func: str              # "SUM" | "COUNT"
    arg: "Expr | None"     # None -> COUNT(*)


Expr = (
    Attr | IntLit | StrLit | NullLit | BoolLit | BinaryExpr | UnaryExpr
    | IsNullExpr | InExpr | BetweenExpr | Aggregate
)


@dataclass
class TableScan:
    name: str               # real table name
    alias: str | None = None


@dataclass
class ExplicitJoin:
    join_type: str   # "INNER" | "LEFT" | "RIGHT" | "FULL"
    lhs: "TableRef"
    rhs: "TableRef"
    on: Expr


@dataclass
class CrossJoin:
    lhs: "TableRef"
    rhs: "TableRef"


TableRef = TableScan | ExplicitJoin | CrossJoin


# A projected output: an expression with an optional `AS alias`.
@dataclass
class Target:
    expr: Expr
    alias: str | None = None


@dataclass
class SelectQuery:
    targets: list[Target]
    from_: TableRef
    where: Expr | None = None
    group_by: list[Attr] = field(default_factory=list)
    order_by: list[tuple[Attr, str]] = field(default_factory=list)  # (key, "asc"|"desc")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scan_bindings(ref: TableRef) -> list[tuple[str, str]]:
    """List of (visible_name, real_table) for every scan in the ref tree."""
    if isinstance(ref, TableScan):
        return [(ref.alias or ref.name, ref.name)]
    return _scan_bindings(ref.lhs) + _scan_bindings(ref.rhs)


# ---------------------------------------------------------------------------
# Random query builder
# ---------------------------------------------------------------------------

_INT_COMPARISON_OPS = ["=", "!=", "<", ">", "<=", ">="]
_STR_COMPARISON_OPS = ["=", "!="]
_LOGICAL_OPS = ["and", "or"]
_JOIN_TYPES = ["INNER", "LEFT", "RIGHT", "FULL"]


class QueryGenerator:
    def __init__(self, schema: Schema, rng: random.Random):
        self.schema = schema
        self.rng = rng
        # Per-query map: visible table name -> real table name.
        self._bindings: dict[str, str] = {}

    # -- attribute / type helpers ------------------------------------------

    def _real_table(self, visible: str) -> str:
        return self._bindings[visible]

    def _column_type(self, attr: Attr) -> str:
        real = self._real_table(attr.table)
        for c in self.schema.tables[real].columns:
            if c.name == attr.column:
                return c.type
        return "int"

    def _samples(self, attr: Attr) -> list:
        real = self._real_table(attr.table)
        return self.schema.tables[real].sample_values.get(attr.column, [])

    def _available_attrs(self) -> list[Attr]:
        attrs = []
        for visible, real in self._bindings.items():
            for col in self.schema.tables[real].columns:
                attrs.append(Attr(visible, col.name))
        return attrs

    # -- top-level ---------------------------------------------------------

    def generate(self) -> SelectQuery:
        from_ = self._gen_from()
        self._bindings = dict(_scan_bindings(from_))
        attrs = self._available_attrs()

        where = self._gen_predicate(attrs) if self.rng.random() < 0.6 else None

        if self.rng.random() < 0.3 and attrs:
            return self._gen_aggregated(from_, attrs, where)

        n_targets = self.rng.randint(1, min(3, len(attrs)))
        chosen = self.rng.sample(attrs, n_targets)
        targets = [Target(a, self._maybe_alias()) for a in chosen]
        order_by = self._gen_order_by(chosen)
        return SelectQuery(targets, from_, where, [], order_by)

    def _gen_aggregated(
        self, from_: TableRef, attrs: list[Attr], where: Expr | None
    ) -> SelectQuery:
        n_group = self.rng.randint(1, min(2, len(attrs)))
        group_by = self.rng.sample(attrs, n_group)

        # Every non-aggregate target must be a group-by column (MS SQL rule).
        targets = [Target(a, self._maybe_alias()) for a in group_by]

        int_attrs = [a for a in attrs if self._column_type(a) == "int"]
        for _ in range(self.rng.randint(0, 2)):
            agg = self._gen_aggregate(int_attrs)
            targets.append(Target(agg, self._maybe_alias()))

        order_by = self._gen_order_by(group_by)
        return SelectQuery(targets, from_, where, group_by, order_by)

    def _gen_aggregate(self, int_attrs: list[Attr]) -> Aggregate:
        choice = self.rng.random()
        if choice < 0.34 and int_attrs:
            return Aggregate("SUM", self.rng.choice(int_attrs))
        if choice < 0.67:
            return Aggregate("COUNT", None)  # COUNT(*)
        return Aggregate("COUNT", self.rng.choice(int_attrs) if int_attrs else None)

    def _maybe_alias(self) -> str | None:
        if self.rng.random() < 0.3:
            return f"c{self.rng.randint(0, 9999)}"
        return None

    def _gen_order_by(self, output_attrs: list[Attr]) -> list[tuple[Attr, str]]:
        # ORDER BY keys restricted to projected output columns (C++ allows only
        # column refs there).
        if not output_attrs or self.rng.random() >= 0.3:
            return []
        n = self.rng.randint(1, len(output_attrs))
        keys = self.rng.sample(output_attrs, n)
        return [(k, self.rng.choice(["asc", "desc"])) for k in keys]

    # -- FROM --------------------------------------------------------------

    def _gen_from(self) -> TableRef:
        names = list(self.schema.tables.keys())
        n = self.rng.randint(1, min(3, len(names)))
        chosen = self.rng.sample(names, n)
        use_alias = self.rng.random() < 0.4

        def make_scan(table: str, idx: int) -> TableScan:
            return TableScan(table, f"t{idx}" if use_alias else None)

        # Track visible name -> real table while building so joins can pick
        # a valid lhs binding.
        scans: list[TableScan] = [make_scan(chosen[0], 0)]
        ref: TableRef = scans[0]

        for i, table_name in enumerate(chosen[1:], start=1):
            rhs_scan = make_scan(table_name, i)
            scans.append(rhs_scan)

            rhs_cols = {c.name for c in self.schema.tables[table_name].columns}
            # Candidate lhs scans that share a column name with rhs.
            candidates = [
                (s, c.name)
                for s in scans[:-1]
                for c in self.schema.tables[s.name].columns
                if c.name in rhs_cols
            ]

            if candidates and self.rng.random() < 0.7:
                lhs_scan, col = self.rng.choice(candidates)
                lhs_vis = lhs_scan.alias or lhs_scan.name
                rhs_vis = rhs_scan.alias or rhs_scan.name
                on: Expr = BinaryExpr("=", Attr(lhs_vis, col), Attr(rhs_vis, col))
                jtype = self.rng.choice(_JOIN_TYPES)
                ref = ExplicitJoin(jtype, ref, rhs_scan, on)
            else:
                ref = CrossJoin(ref, rhs_scan)

        return ref

    # -- WHERE -------------------------------------------------------------

    def _gen_predicate(self, attrs: list[Attr], depth: int = 0) -> Expr:
        if depth >= 2 or self.rng.random() < 0.45:
            return self._gen_comparison(attrs)

        op = self.rng.choice(_LOGICAL_OPS)
        lhs = self._gen_predicate(attrs, depth + 1)
        rhs = self._gen_predicate(attrs, depth + 1)
        return BinaryExpr(op, lhs, rhs)

    def _gen_comparison(self, attrs: list[Attr]) -> Expr:
        attr = self.rng.choice(attrs)
        is_string = self._column_type(attr) == "string"

        roll = self.rng.random()
        if roll < 0.1:
            return IsNullExpr(attr, negated=self.rng.random() < 0.5)
        if roll < 0.3:
            return self._gen_in(attr)
        if roll < 0.4 and not is_string:
            return self._gen_between(attr)

        ops = _STR_COMPARISON_OPS if is_string else _INT_COMPARISON_OPS
        op = self.rng.choice(ops)
        return BinaryExpr(op, attr, self._gen_literal(attr))

    def _gen_in(self, attr: Attr) -> InExpr:
        n = self.rng.randint(1, 4)
        values = [self._gen_literal(attr) for _ in range(n)]
        # Emit sorted ascending so the OR-chain MS SQL expands matches ours.
        values.sort(key=lambda v: v.value)
        return InExpr(attr, values, negated=self.rng.random() < 0.3)

    def _gen_between(self, attr: Attr) -> BetweenExpr:
        lo = self._gen_literal(attr)
        hi = self._gen_literal(attr)
        if lo.value > hi.value:
            lo, hi = hi, lo
        return BetweenExpr(attr, lo, hi, negated=self.rng.random() < 0.3)

    def _gen_literal(self, attr: Attr) -> Expr:
        sample = self._samples(attr)
        if self._column_type(attr) == "string":
            if sample and self.rng.random() < 0.8:
                return StrLit(self.rng.choice(sample))
            return StrLit(self.rng.choice(["AMERICA", "EUROPE", "ASIA", "unknown"]))
        if sample and self.rng.random() < 0.75:
            return IntLit(self.rng.choice(sample))
        return IntLit(self.rng.randint(1, 100))


# ---------------------------------------------------------------------------
# Dialect protocol
# ---------------------------------------------------------------------------

class Dialect(Protocol):
    def fmt_table(self, name: str) -> str: ...
    def fmt_col_ref(self, table: str, col: str) -> str: ...
    def fmt_not_equals(self) -> str: ...
    def fmt_join_keyword(self, join_type: str) -> str: ...


class PostgresSubsetDialect:
    """Plain identifiers, no schema prefix.  Matches the C++ ANTLR visitor."""

    def fmt_table(self, name: str) -> str:
        return name

    def fmt_col_ref(self, table: str, col: str) -> str:
        return f"{table}.{col}"

    def fmt_not_equals(self) -> str:
        return "!="

    def fmt_join_keyword(self, join_type: str) -> str:
        return {
            "INNER": "JOIN",
            "LEFT":  "LEFT JOIN",
            "RIGHT": "RIGHT JOIN",
            "FULL":  "FULL JOIN",
        }[join_type]


class MsSqlDialect:
    """Bracket-quoted identifiers with an optional schema prefix."""

    def __init__(self, schema_prefix: str = "dbo"):
        self._schema = schema_prefix

    def fmt_table(self, name: str) -> str:
        if self._schema:
            return f"{self._schema}.{name}"
        return name

    def fmt_col_ref(self, table: str, col: str) -> str:
        return f"{table}.{col}"

    def fmt_not_equals(self) -> str:
        return "<>"

    def fmt_join_keyword(self, join_type: str) -> str:
        return {
            "INNER": "INNER JOIN",
            "LEFT":  "LEFT OUTER JOIN",
            "RIGHT": "RIGHT OUTER JOIN",
            "FULL":  "FULL OUTER JOIN",
        }[join_type]


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

def _fmt_str_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_expr(expr: Expr, d: Dialect) -> str:
    match expr:
        case Attr(table, col):
            return d.fmt_col_ref(table, col)
        case IntLit(value):
            return str(value)
        case StrLit(value):
            return _fmt_str_literal(value)
        case NullLit():
            return "NULL"
        case BoolLit(value):
            return "TRUE" if value else "FALSE"
        case Aggregate("COUNT", None):
            return "COUNT(*)"
        case Aggregate(func, arg):
            return f"{func}({render_expr(arg, d)})"
        case InExpr(attr, values, negated):
            kw = "NOT IN" if negated else "IN"
            vals = ", ".join(render_expr(v, d) for v in values)
            return f"{render_expr(attr, d)} {kw} ({vals})"
        case BetweenExpr(attr, lo, hi, negated):
            kw = "NOT BETWEEN" if negated else "BETWEEN"
            return f"{render_expr(attr, d)} {kw} {render_expr(lo, d)} AND {render_expr(hi, d)}"
        case BinaryExpr(op, lhs, rhs):
            l = render_expr(lhs, d)
            r = render_expr(rhs, d)
            if op == "!=":
                op = d.fmt_not_equals()
            elif op in ("and", "or"):
                op = op.upper()
                return f"({l} {op} {r})"
            return f"{l} {op} {r}"
        case UnaryExpr("not", child):
            return f"NOT ({render_expr(child, d)})"
        case UnaryExpr("uminus", child):
            return f"-{render_expr(child, d)}"
        case IsNullExpr(child, negated):
            c = render_expr(child, d)
            return f"{c} IS NOT NULL" if negated else f"{c} IS NULL"
    raise ValueError(f"unhandled expr type: {type(expr)}")


def render_table_ref(ref: TableRef, d: Dialect) -> str:
    match ref:
        case TableScan(name, alias):
            base = d.fmt_table(name)
            return f"{base} AS {alias}" if alias else base
        case CrossJoin(lhs, rhs):
            return f"{render_table_ref(lhs, d)} CROSS JOIN {render_table_ref(rhs, d)}"
        case ExplicitJoin(jtype, lhs, rhs, on):
            kw = d.fmt_join_keyword(jtype)
            return (
                f"{render_table_ref(lhs, d)}"
                f" {kw} {render_table_ref(rhs, d)}"
                f" ON {render_expr(on, d)}"
            )
    raise ValueError(f"unhandled ref type: {type(ref)}")


def render_query(query: SelectQuery, d: Dialect) -> str:
    target_parts = []
    for t in query.targets:
        rendered = render_expr(t.expr, d)
        if t.alias:
            rendered += f" AS {t.alias}"
        target_parts.append(rendered)
    targets = ", ".join(target_parts)

    from_clause = render_table_ref(query.from_, d)
    sql = f"SELECT {targets}\nFROM {from_clause}"
    if query.where is not None:
        sql += f"\nWHERE {render_expr(query.where, d)}"
    if query.group_by:
        cols = ", ".join(d.fmt_col_ref(a.table, a.column) for a in query.group_by)
        sql += f"\nGROUP BY {cols}"
    if query.order_by:
        keys = ", ".join(
            f"{d.fmt_col_ref(a.table, a.column)} {direction.upper()}"
            for a, direction in query.order_by
        )
        sql += f"\nORDER BY {keys}"
    return sql


# ---------------------------------------------------------------------------
# Registry — add new dialects here
# ---------------------------------------------------------------------------

DIALECTS: dict[str, Dialect] = {
    "pg":    PostgresSubsetDialect(),
    "mssql": MsSqlDialect(),
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        default="../test/static/executor/test_data",
        help="Directory containing *.csv table files",
    )
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument(
        "--dialect",
        choices=list(DIALECTS),
        default=None,
        help="Print only one dialect. Omit to print all.",
    )
    args = parser.parse_args()

    schema = load_schema(Path(args.data_dir))
    print(f"Loaded tables: {', '.join(schema.tables)}\n")

    rng = random.Random(args.seed)
    gen = QueryGenerator(schema, rng)

    dialects_to_print: dict[str, Dialect] = (
        {args.dialect: DIALECTS[args.dialect]} if args.dialect else DIALECTS
    )

    for i in range(1, args.count + 1):
        query = gen.generate()
        print(f"-- Query {i} " + "-" * 60)
        for name, dialect in dialects_to_print.items():
            print(f"-- [{name}]")
            print(render_query(query, dialect))
            print()


if __name__ == "__main__":
    main()
