"""
Run queries against MS SQL Server for the differential fuzzer.

Responsibilities:
  - setup_schema(data_dir): drop+recreate the fuzzer schema, mirror every
    *.csv in data_dir as a `dbo.<table>(... INT NULL)` table, and bulk-load
    rows.
  - run(query): execute one SELECT and return columns/rows or a structured
    error.

The connection is reused across calls so the fuzz loop pays one TCP/login cost
per session, not per query.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

import pymssql


_BENCH_RE = re.compile(r"_\d+$")


@dataclass
class RunResult:
    columns: list[str]          # column names in projection order
    rows: list[tuple]           # values as returned by the driver
    error: str | None = None    # set when execution failed; columns/rows empty


class MsSqlRunner:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1433,
        user: str = "sa",
        password: str = "Password123!",
        database: str = "fuzz",
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._conn = None  # opened lazily after the database exists

    def _connect(self, database: str):
        return pymssql.connect(
            server=self._host,
            port=str(self._port),
            user=self._user,
            password=self._password,
            database=database,
            autocommit=True,
        )

    def _ensure_database(self) -> None:
        with self._connect("master") as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sys.databases WHERE name=%s", (self._database,))
            if cur.fetchone()[0] == 0:
                cur.execute(f"CREATE DATABASE [{self._database}]")

    def setup_schema(self, data_dir: str | Path) -> None:
        """
        Drop everything under dbo and recreate one INT-NULL table per *.csv
        in data_dir (skipping the benchmark-only ``_<digits>`` siblings — they
        share the base table's schema).
        """
        data_dir = Path(data_dir)
        self._ensure_database()
        self._conn = self._connect(self._database)
        cur = self._conn.cursor()

        # Drop everything we own so reruns are idempotent.
        cur.execute("""
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += 'DROP TABLE [dbo].[' + t.name + ']; '
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = 'dbo';
            EXEC sp_executesql @sql;
        """)

        for path in sorted(data_dir.glob("*.csv")):
            if _BENCH_RE.search(path.stem):
                continue
            self._create_and_load(cur, path)

    _SQL_TYPE = {"int": "INT NULL", "string": "VARCHAR(255) NULL"}

    def _create_and_load(self, cur, path: Path) -> None:
        with path.open() as f:
            reader = csv.reader(f)
            header = next(reader)
            cols = []
            types = []
            for h in header:
                name, type_ = (s.strip() for s in h.split(":"))
                if type_ not in self._SQL_TYPE:
                    raise ValueError(f"{path}: unsupported column type {type_!r}")
                cols.append(name)
                types.append(type_)

            def conv(v: str, type_: str):
                v = v.strip()
                if v == "NULL":
                    return None
                return int(v) if type_ == "int" else v

            rows = [tuple(conv(v, t) for v, t in zip(r, types)) for r in reader]

        col_defs = ", ".join(f"[{c}] {self._SQL_TYPE[t]}" for c, t in zip(cols, types))
        cur.execute(f"CREATE TABLE [dbo].[{path.stem}] ({col_defs})")

        if rows:
            placeholders = ", ".join(["%s"] * len(cols))
            cur.executemany(
                f"INSERT INTO [dbo].[{path.stem}] VALUES ({placeholders})",
                rows,
            )

    def run(self, query: str) -> RunResult:
        if self._conn is None:
            self._conn = self._connect(self._database)
        cur = self._conn.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description] if cur.description else []
            return RunResult(columns=columns, rows=rows)
        except pymssql.Error as e:
            return RunResult(columns=[], rows=[], error=str(e))

    def get_plan(self, query: str) -> str:
        """Return the ShowPlanXML for `query`. Use OPTION (RECOMPILE) to inline literals."""
        if self._conn is None:
            self._conn = self._connect(self._database)
        cur = self._conn.cursor()
        cur.execute("SET STATISTICS XML ON")
        try:
            cur.execute(query)
            while cur.nextset():
                row = cur.fetchone()
                if row and isinstance(row[0], str) and row[0].startswith("<ShowPlanXML"):
                    return row[0]
            raise RuntimeError("No execution plan returned by SQL Server")
        finally:
            cur.execute("SET STATISTICS XML OFF")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
