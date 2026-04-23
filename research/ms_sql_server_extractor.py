import re
from pathlib import Path

import pymssql

from extractor import PlanExtractor

_SUBMODULE = Path(__file__).parent / "imdb-sqlserver"

# (tsv filename, raw staging table, column count)
_RAW_FILES = [
    ("name.basics.tsv",       "[Raw].[name.basics.tsv.gz]",      6),
    ("title.basics.tsv",      "[Raw].[title.basics.tsv.gz]",     9),
    ("title.akas.tsv",        "[Raw].[title.akas.tsv.gz]",       8),
    ("title.crew.tsv",        "[Raw].[title.crew.tsv.gz]",       3),
    ("title.episode.tsv",     "[Raw].[title.episode.tsv.gz]",    4),
    ("title.principals.tsv",  "[Raw].[title.principals.tsv.gz]", 6),
    ("title.ratings.tsv",     "[Raw].[title.ratings.tsv.gz]",    3),
]


def _run_sql_file(cursor, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    for stmt in re.split(r'^\s*GO\s*$', sql, flags=re.MULTILINE | re.IGNORECASE):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


def _drop_raw_pks(cursor) -> None:
    cursor.execute("""
        SELECT kc.name, t.name
        FROM sys.key_constraints kc
        JOIN sys.tables t ON t.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'Raw' AND kc.type = 'PK'
    """)
    rows = cursor.fetchall()
    for pk_name, table_name in rows:
        cursor.execute(f"ALTER TABLE [Raw].[{table_name}] DROP CONSTRAINT [{pk_name}]")


def _get_raw_columns(cursor, raw_table_name: str) -> list[str]:
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'Raw' AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (raw_table_name,))
    return [row[0] for row in cursor.fetchall()]


def _load_raw_table(cursor, tsv_path: Path, raw_table: str, n_cols: int, server_path: str, max_rows: int | None = None) -> None:
    if not tsv_path.exists():
        print(f"  skipping {tsv_path.name} (not found)")
        return

    # raw_table looks like [Raw].[name.basics.tsv.gz] — extract just the table name
    raw_table_name = raw_table[len("[Raw].["):-1]
    stage_table = f"[Stage].[{raw_table_name}]"

    col_defs = ", ".join(f"c{i} varchar(max) NULL" for i in range(1, n_cols + 1))
    cursor.execute(f"CREATE TABLE {stage_table} ({col_defs})")

    lastrow_clause = f"LASTROW = {max_rows + 1}," if max_rows is not None else ""
    cursor.execute(f"""
        BULK INSERT {stage_table}
        FROM '{server_path}'
        WITH (
            FIRSTROW        = 2,
            {lastrow_clause}
            FIELDTERMINATOR = '\\t',
            ROWTERMINATOR   = '0x0a',
            TABLOCK
        )
    """)

    col_names = _get_raw_columns(cursor, raw_table_name)
    raw_cols = ", ".join(f"[{c}]" for c in col_names)
    nullif_sel = ", ".join(f"NULLIF(c{i}, '\\N')" for i in range(1, n_cols + 1))

    cursor.execute(f"""
        INSERT INTO {raw_table} WITH (TABLOCK) ({raw_cols})
        SELECT {nullif_sel}
        FROM {stage_table}
    """)

    cursor.execute(f"DROP TABLE {stage_table}")


class MsSqlServerExtractor(PlanExtractor):
    def __init__(
        self,
        dataset: str,
        host: str,
        port: int,
        user: str,
        password: str,
        server_dataset: str = "/datasets/mysql-data/imdb",
        max_rows: int | None = None,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._server_dataset = server_dataset.rstrip("/")
        self._max_rows = max_rows
        super().__init__(dataset)

    def _conn(self, database="master"):
        c = pymssql.connect(
            server=self._host,
            port=str(self._port),
            user=self._user,
            password=self._password,
            database=database,
            autocommit=True,
        )
        return c

    def load_dataset(self) -> None:
        dataset_dir = Path(self.dataset)

        print("Recreating database...")
        with self._conn("master") as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sys.databases WHERE name=N'imdb'")
            row = cur.fetchone()
            if row and row[0]:
                cur.execute("ALTER DATABASE imdb SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                cur.execute("DROP DATABASE imdb")
            cur.execute("CREATE DATABASE imdb")

        with self._conn("imdb") as conn:
            cur = conn.cursor()

            print("Creating schema and tables...")
            _run_sql_file(cur, _SUBMODULE / "Create IMDB-schema.sql")

            cur.execute("CREATE SCHEMA [Stage]")
            _drop_raw_pks(cur)

            for tsv_name, raw_table, n_cols in _RAW_FILES:
                print(f"Loading {tsv_name}...")
                server_path = f"{self._server_dataset}/{tsv_name}"
                _load_raw_table(cur, dataset_dir / tsv_name, raw_table, n_cols, server_path, self._max_rows)

            cur.execute("DROP SCHEMA [Stage]")

            print("Transforming into relational tables...")
            if self._max_rows is not None:
                cur.execute("""
                    DECLARE @sql NVARCHAR(MAX) = '';
                    SELECT @sql += 'ALTER TABLE [dbo].[' + t.name + '] NOCHECK CONSTRAINT ALL; '
                    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dbo';
                    EXEC(@sql)
                """)
            _run_sql_file(cur, _SUBMODULE / "Load IMDB relational tables.sql")

        print("Dataset loaded.")

    def extract(self, request: str) -> str:
        with self._conn("imdb") as conn:
            cur = conn.cursor()
            cur.execute("SET STATISTICS XML ON")
            cur.execute(request)
            while cur.nextset():
                row = cur.fetchone()
                if row and isinstance(row[0], str) and row[0].startswith("<ShowPlanXML"):
                    return row[0]
        raise RuntimeError("No execution plan returned by SQL Server")
