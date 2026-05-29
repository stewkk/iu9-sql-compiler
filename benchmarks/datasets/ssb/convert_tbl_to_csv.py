#!/usr/bin/env python3
"""Convert Star Schema Benchmark .tbl files to this project's CSV format."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SUPPORTED_TYPES = {"int", "string"}


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]


class ConversionError(RuntimeError):
    pass


def load_schema(path: Path) -> list[Table]:
    with path.open(encoding="utf-8") as f:
        raw = json.load(f)

    tables = []
    for table_raw in raw.get("tables", []):
        columns = []
        for column_raw in table_raw.get("columns", []):
            column_type = column_raw["type"]
            if column_type not in SUPPORTED_TYPES:
                raise ConversionError(
                    f"{path}: unsupported type {column_type!r} in table {table_raw['name']}"
                )
            columns.append(Column(column_raw["name"], column_type))
        if not columns:
            raise ConversionError(f"{path}: table {table_raw.get('name')!r} has no columns")
        tables.append(Table(table_raw["name"], tuple(columns)))

    if not tables:
        raise ConversionError(f"{path}: schema does not define any tables")
    return tables


def parse_tbl_line(line: str) -> list[str]:
    fields = line.rstrip("\r\n").split("|")
    if fields and fields[-1] == "":
        fields.pop()
    return fields


def validate_row(table: Table, row: list[str], input_path: Path, line_no: int) -> None:
    if len(row) != len(table.columns):
        raise ConversionError(
            f"{input_path}:{line_no}: expected {len(table.columns)} fields for "
            f"{table.name}, got {len(row)}"
        )

    for value, column in zip(row, table.columns, strict=True):
        if column.type == "int":
            try:
                int(value)
            except ValueError as exc:
                raise ConversionError(
                    f"{input_path}:{line_no}: {column.name} expects int, got {value!r}"
                ) from exc


def header(table: Table) -> list[str]:
    return [f"{column.name}:{column.type}" for column in table.columns]


def convert_table(table: Table, input_dir: Path, output_dir: Path) -> int:
    input_path = input_dir / f"{table.name}.tbl"
    output_path = output_dir / f"{table.name}.csv"
    if not input_path.exists():
        raise ConversionError(f"missing input file: {input_path}")

    rows = 0
    with input_path.open(encoding="utf-8", newline="") as src, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        writer = csv.writer(dst, lineterminator="\n")
        writer.writerow(header(table))
        for line_no, line in enumerate(src, start=1):
            row = parse_tbl_line(line)
            validate_row(table, row, input_path, line_no)
            writer.writerow(row)
            rows += 1
    return rows


def convert_all(schema_path: Path, input_dir: Path, output_dir: Path) -> dict[str, int]:
    tables = load_schema(schema_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return {
        table.name: convert_table(table, input_dir, output_dir)
        for table in tables
    }


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    default_schema = Path(__file__).with_name("schema.json")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path, help="Directory containing SSB .tbl files")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for generated CSV files")
    parser.add_argument("--schema", default=default_schema, type=Path, help="Schema JSON path")
    return parser.parse_args(argv)


def main(argv: Iterable[str] = sys.argv[1:]) -> int:
    args = parse_args(argv)
    try:
        counts = convert_all(args.schema, args.input_dir, args.output_dir)
    except ConversionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    for table_name, rows in counts.items():
        print(f"wrote {args.output_dir / (table_name + '.csv')} ({rows} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
