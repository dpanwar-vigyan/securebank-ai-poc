"""
Analytics DB adapter layer — swap between ClickHouse and DuckDB/MotherDuck
via ANALYTICS_DB_BACKEND environment variable.

Supported backends:
  clickhouse  (default) — ClickHouse Cloud via clickhouse-connect
  motherduck            — DuckDB + MotherDuck cloud (free tier, ~$0/month for demo)

Usage:
    from rag.db_adapters import get_adapter
    db = get_adapter()
    rows = db.query("SELECT ...", parameters={"key": "val"})
    db.command("UPDATE ...")
    db.insert("schema.table", [[...], [...]], ["col1", "col2"])

All adapters expose the same interface — callers never import a backend directly.
"""

import os
import re
from abc import ABC, abstractmethod


# ── Normalised result container ────────────────────────────────────────────────

class DBRow:
    """Uniform query result — same shape regardless of backend."""
    def __init__(self, rows: list, columns: list[str]):
        self.result_rows  = rows          # list of tuples
        self.column_names = columns       # list of str
        self.result_set   = rows          # alias used by some callers


# ── Abstract interface ─────────────────────────────────────────────────────────

class AnalyticsDBAdapter(ABC):

    @abstractmethod
    def ping(self) -> bool: ...

    @abstractmethod
    def query(self, sql: str, parameters: dict | None = None) -> DBRow: ...

    @abstractmethod
    def command(self, sql: str, parameters: dict | None = None) -> None: ...

    @abstractmethod
    def insert(self, table: str, data: list[list], column_names: list[str]) -> None: ...


# ── ClickHouse adapter (wraps existing clickhouse-connect, zero behaviour change) ──

class ClickHouseAdapter(AnalyticsDBAdapter):

    def __init__(self):
        import clickhouse_connect
        self._ch = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True,
            connect_timeout=8,
            send_receive_timeout=30,
        )

    def ping(self) -> bool:
        self._ch.ping()
        return True

    def query(self, sql: str, parameters: dict | None = None) -> DBRow:
        result = self._ch.query(sql, parameters=parameters or {})
        return DBRow(result.result_rows, list(result.column_names))

    def command(self, sql: str, parameters: dict | None = None) -> None:
        self._ch.command(sql, parameters=parameters or {})

    def insert(self, table: str, data: list[list], column_names: list[str]) -> None:
        self._ch.insert(table=table, data=data, column_names=column_names)


# ── DuckDB / MotherDuck adapter ────────────────────────────────────────────────

class DuckDBAdapter(AnalyticsDBAdapter):
    """
    Works with:
      - MotherDuck cloud:  MOTHERDUCK_TOKEN set (free tier at motherduck.com)
      - Local DuckDB file: DUCKDB_PATH set        (dev / CI)
      - In-memory:         neither set             (tests)

    Handles two ClickHouse-isms automatically:
      1. Parameter syntax  {key:Type}  →  positional ?
      2. Mutation syntax   ALTER TABLE t UPDATE col=v WHERE …  →  UPDATE t SET col=v WHERE …
    """

    def __init__(self):
        import duckdb
        token   = os.getenv("MOTHERDUCK_TOKEN", "")
        db_name = os.getenv("MOTHERDUCK_DB", "askmybank")
        if token:
            # MotherDuck cloud: connect to root first, create DB if needed, then reconnect
            root = duckdb.connect(f"md:?motherduck_token={token}")
            root.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            root.close()
            self._con = duckdb.connect(f"md:{db_name}?motherduck_token={token}")
        else:
            path = os.getenv("DUCKDB_PATH", ":memory:")
            self._con = duckdb.connect(path)
        self._con.execute("CREATE SCHEMA IF NOT EXISTS banking_docs")

    # ── param + mutation helpers ───────────────────────────────────────────────

    @staticmethod
    def _translate(sql: str, parameters: dict) -> tuple[str, list]:
        """
        Convert ClickHouse {key:Type} params → positional ? params.
        Returns (translated_sql, ordered_values).
        """
        keys   = re.findall(r"\{(\w+):[^}]+\}", sql)
        values = [parameters[k] for k in keys]
        sql    = re.sub(r"\{(\w+):[^}]+\}", "?", sql)
        return sql, values

    @staticmethod
    def _mutate(sql: str) -> str:
        """
        Convert ClickHouse ALTER TABLE mutation to standard SQL UPDATE.
        ALTER TABLE t UPDATE col = x WHERE … → UPDATE t SET col = x WHERE …
        Also strips FINAL keyword (ReplacingMergeTree — not needed in DuckDB).
        """
        sql = re.sub(
            r"ALTER\s+TABLE\s+(\S+)\s+UPDATE\s+",
            r"UPDATE \1 SET ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(r"\bFINAL\b", "", sql)
        return sql

    # ── public interface ───────────────────────────────────────────────────────

    def ping(self) -> bool:
        self._con.execute("SELECT 1")
        return True

    def query(self, sql: str, parameters: dict | None = None) -> DBRow:
        sql = self._mutate(sql)
        if parameters:
            sql, values = self._translate(sql, parameters)
            cur = self._con.execute(sql, values)
        else:
            cur = self._con.execute(sql)
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []
        return DBRow(rows, columns)

    def command(self, sql: str, parameters: dict | None = None) -> None:
        sql = self._mutate(sql)
        if parameters:
            sql, values = self._translate(sql, parameters)
            self._con.execute(sql, values)
        else:
            self._con.execute(sql)

    def insert(self, table: str, data: list[list], column_names: list[str]) -> None:
        placeholders = ", ".join("?" for _ in column_names)
        cols         = ", ".join(column_names)
        sql          = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
        self._con.executemany(sql, data)


# ── Factory ────────────────────────────────────────────────────────────────────

def get_adapter() -> AnalyticsDBAdapter:
    """Return the configured backend adapter. Driven by ANALYTICS_DB_BACKEND env var."""
    backend = os.getenv("ANALYTICS_DB_BACKEND", "clickhouse").lower()
    if backend in ("motherduck", "duckdb"):
        return DuckDBAdapter()
    return ClickHouseAdapter()
