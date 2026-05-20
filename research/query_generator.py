#!/usr/bin/env python3
"""
Random SQL query generator.

Builds queries in a strict SELECT-Project-Filter-Join (SPJ) subset that matches
what the C++ parser accepts, then renders them in pluggable SQL dialects.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

@dataclass
class Column:
    name: str
    type: str  # "int" only in current C++ implementation


@dataclass
class TableSchema:
    name: str
    columns: list[Column]
    sample_values: dict[str, list[int]]  # col_name -> sampled actual values


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

            sample_values: dict[str, list[int]] = {c.name: [] for c in columns}
            for i, row in enumerate(reader):
                if i >= max_sample:
                    break
                for col, val in zip(columns, row):
                    v = val.strip()
                    if v != "NULL":
                        try:
                            sample_values[col.name].append(int(v))
                        except ValueError:
                            pass

        tables[path.stem] = TableSchema(path.stem, columns, sample_values)
    return Schema(tables)


# ---------------------------------------------------------------------------
# Query IR — only constructs that the C++ parser accepts
# ---------------------------------------------------------------------------

@dataclass
class Attr:
    table: str
    column: str


@dataclass
class IntLit:
    value: int


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


Expr = Attr | IntLit | NullLit | BoolLit | BinaryExpr | UnaryExpr | IsNullExpr


@dataclass
class TableScan:
    name: str


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


@dataclass
class SelectQuery:
    targets: list[Attr]
    from_: TableRef
    where: Expr | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tables_in_ref(ref: TableRef) -> list[str]:
    if isinstance(ref, TableScan):
        return [ref.name]
    return _tables_in_ref(ref.lhs) + _tables_in_ref(ref.rhs)


def _available_attrs(schema: Schema, ref: TableRef) -> list[Attr]:
    return [
        Attr(t, col.name)
        for t in _tables_in_ref(ref)
        if t in schema.tables
        for col in schema.tables[t].columns
    ]


# ---------------------------------------------------------------------------
# Random query builder
# ---------------------------------------------------------------------------

_COMPARISON_OPS = ["=", "!=", "<", ">", "<=", ">="]
_LOGICAL_OPS = ["and", "or"]
_ARITHMETIC_OPS = ["+", "-", "*"]
_JOIN_TYPES = ["INNER", "LEFT", "RIGHT", "FULL"]


class QueryGenerator:
    def __init__(self, schema: Schema, rng: random.Random):
        self.schema = schema
        self.rng = rng

    def generate(self) -> SelectQuery:
        from_ = self._gen_from()
        attrs = _available_attrs(self.schema, from_)

        n_targets = self.rng.randint(1, min(3, len(attrs)))
        targets = self.rng.sample(attrs, n_targets)

        where = self._gen_predicate(attrs) if self.rng.random() < 0.6 else None
        return SelectQuery(targets, from_, where)

    def _gen_from(self) -> TableRef:
        names = list(self.schema.tables.keys())
        n = self.rng.randint(1, min(3, len(names)))
        chosen = self.rng.sample(names, n)

        ref: TableRef = TableScan(chosen[0])
        for table_name in chosen[1:]:
            rhs: TableRef = TableScan(table_name)

            # Try equi-join on a shared column name first.
            lhs_col_names = {
                c.name
                for t in _tables_in_ref(ref)
                for c in self.schema.tables[t].columns
            }
            rhs_col_names = {c.name for c in self.schema.tables[table_name].columns}
            shared = sorted(lhs_col_names & rhs_col_names)

            if shared and self.rng.random() < 0.7:
                col = self.rng.choice(shared)
                # Pick a LHS table that actually has this column.
                lhs_candidate = self.rng.choice([
                    t for t in _tables_in_ref(ref)
                    if any(c.name == col for c in self.schema.tables[t].columns)
                ])
                on: Expr = BinaryExpr("=", Attr(lhs_candidate, col), Attr(table_name, col))
                jtype = self.rng.choice(_JOIN_TYPES)
                ref = ExplicitJoin(jtype, ref, rhs, on)
            else:
                ref = CrossJoin(ref, rhs)

        return ref

    def _gen_predicate(self, attrs: list[Attr], depth: int = 0) -> Expr:
        # Deeper recursion becomes a leaf comparison to avoid runaway trees.
        if depth >= 2 or self.rng.random() < 0.45:
            return self._gen_comparison(attrs)

        op = self.rng.choice(_LOGICAL_OPS)
        lhs = self._gen_predicate(attrs, depth + 1)
        rhs = self._gen_predicate(attrs, depth + 1)
        return BinaryExpr(op, lhs, rhs)

    def _gen_comparison(self, attrs: list[Attr]) -> Expr:
        attr = self.rng.choice(attrs)

        if self.rng.random() < 0.12:
            return IsNullExpr(attr, negated=self.rng.random() < 0.5)

        op = self.rng.choice(_COMPARISON_OPS)
        rhs = self._gen_int_expr(attr)
        return BinaryExpr(op, attr, rhs)

    def _gen_int_expr(self, hint_attr: Attr) -> Expr:
        sample = self.schema.tables[hint_attr.table].sample_values.get(hint_attr.column, [])
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

def render_expr(expr: Expr, d: Dialect) -> str:
    match expr:
        case Attr(table, col):
            return d.fmt_col_ref(table, col)
        case IntLit(value):
            return str(value)
        case NullLit():
            return "NULL"
        case BoolLit(value):
            return "TRUE" if value else "FALSE"
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
        case TableScan(name):
            return d.fmt_table(name)
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
    targets = ", ".join(d.fmt_col_ref(a.table, a.column) for a in query.targets)
    from_clause = render_table_ref(query.from_, d)
    sql = f"SELECT {targets}\nFROM {from_clause}"
    if query.where is not None:
        sql += f"\nWHERE {render_expr(query.where, d)}"
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
