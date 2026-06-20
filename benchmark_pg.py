#!/usr/bin/env python3
"""Postgres benchmark script.

This script connects to a PostgreSQL-compatible server and measures timings for:
- CREATE TABLE
- INSERT rows
- a representative set of SELECT/search queries

Usage:
    python benchmark_pg.py --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --password secret

The script will drop and recreate a temporary benchmark table.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError as first_err:
    try:
        import psycopg2 as psycopg
        from psycopg2.extras import RealDictCursor as dict_row
    except ImportError:
        print("Error: install psycopg or psycopg2 to run this benchmark.", file=sys.stderr)
        print("Reason:", first_err, file=sys.stderr)
        sys.exit(1)

BENCH_TABLE = "bench_search"

QUERY_DEFINITIONS: List[Tuple[str, str]] = [
    (
        "select_all",
        f"SELECT * FROM {BENCH_TABLE}",
    ),
    (
        "select_where_age",
        f"SELECT id, name, age FROM {BENCH_TABLE} WHERE age >= 18",
    ),
    (
        "select_where_active",
        f"SELECT id, name FROM {BENCH_TABLE} WHERE active = TRUE",
    ),
    (
        "select_order_limit_offset",
        f"SELECT id, name, age FROM {BENCH_TABLE} WHERE age >= 18 ORDER BY name DESC LIMIT 10 OFFSET 5",
    ),
    (
        "select_like_and_active",
        f"SELECT id, name FROM {BENCH_TABLE} WHERE name LIKE 'user%' AND active = TRUE",
    ),
    (
        "select_between_or",
        f"SELECT id, name, age FROM {BENCH_TABLE} WHERE (age BETWEEN 20 AND 40) OR active = FALSE",
    ),
    (
        "select_count_active",
        f"SELECT COUNT(*) AS count_active FROM {BENCH_TABLE} WHERE active = TRUE",
    ),
    (
        "select_nullable",
        f"SELECT id, name FROM {BENCH_TABLE} WHERE name IS NOT NULL ORDER BY age ASC LIMIT 20",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Postgres benchmark script")
    parser.add_argument("--host", default="127.0.0.1", help="Postgres host")
    parser.add_argument("--port", default=5432, type=int, help="Postgres port")
    parser.add_argument("--dbname", default="postgres", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="", help="Database password")
    parser.add_argument("--rows", default=10000, type=int, help="Number of rows to insert")
    parser.add_argument("--repeat", default=3, type=int, help="Repeat each query N times and average")
    return parser.parse_args()


def connect(args: argparse.Namespace):
    if hasattr(psycopg, "connect"):
        if "psycopg2" in sys.modules:
            conn = psycopg.connect(
                host=args.host,
                port=args.port,
                dbname=args.dbname,
                user=args.user,
                password=args.password,
            )
            conn.autocommit = True
            return conn
        else:
            conn = psycopg.connect(
                host=args.host,
                port=args.port,
                dbname=args.dbname,
                user=args.user,
                password=args.password,
                row_factory=dict_row,
            )
            conn.autocommit = True
            return conn
    raise RuntimeError("Unsupported postgres adapter")


def timed_execute(cursor, sql: str) -> float:
    start = time.perf_counter()
    cursor.execute(sql)
    elapsed = time.perf_counter() - start
    return elapsed


def create_benchmark_table(cursor) -> float:
    cursor.execute(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
    create_sql = f"""
    CREATE TABLE {BENCH_TABLE} (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        active BOOLEAN NOT NULL,
        note TEXT
    )
    """
    return timed_execute(cursor, create_sql)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def insert_rows(cursor, row_count: int) -> float:
    total = 0.0
    for row_id in range(1, row_count + 1):
        active = row_id % 3 != 0
        name = f"user_{row_id:07d}"
        age = 18 + (row_id % 60)
        note = f"note_{row_id}"
        active_sql = "TRUE" if active else "FALSE"
        insert_sql = (
            f"INSERT INTO {BENCH_TABLE} (id, name, age, active, note) VALUES "
            f"({row_id}, {sql_literal(name)}, {age}, {active_sql}, {sql_literal(note)})"
        )
        total += timed_execute(cursor, insert_sql)
    return total


def insert_rows_batched(cursor, row_count: int) -> float:
    values_list = []
    for row_id in range(1, row_count + 1):
        active = row_id % 3 != 0
        name = f"user_{row_id:07d}"
        age = 18 + (row_id % 60)
        note = f"note_{row_id}"
        active_sql = "TRUE" if active else "FALSE"
        values_list.append(
            f"({row_id}, {sql_literal(name)}, {age}, {active_sql}, {sql_literal(note)})"
        )
    
    insert_sql = (
        f"INSERT INTO {BENCH_TABLE} (id, name, age, active, note) VALUES "
        + ", ".join(values_list)
    )
    return timed_execute(cursor, insert_sql)


def benchmark_query(cursor, label: str, sql: str, repeat: int = 3) -> Tuple[float, float]:
    durations: List[float] = []
    for _ in range(repeat):
        duration = timed_execute(cursor, sql)
        durations.append(duration)
    return min(durations), sum(durations) / len(durations)


def run_benchmarks(args: argparse.Namespace) -> None:
    conn = connect(args)
    try:
        with conn.cursor() as cursor:
            create_time = create_benchmark_table(cursor)
            print(f"create_table: {create_time:.6f}s")

            insert_time = insert_rows(cursor, args.rows)
            print(f"insert_{args.rows}_rows_total: {insert_time:.6f}s")
            print(f"insert_{args.rows}_rows_avg: {insert_time / args.rows:.6f}s")

            # Recreate table for batched insert test
            create_benchmark_table(cursor)
            batched_insert_time = insert_rows_batched(cursor, args.rows)
            print(f"insert_{args.rows}_rows_batched: {batched_insert_time:.6f}s")

            for label, sql in QUERY_DEFINITIONS:
                minimum, avg = benchmark_query(cursor, label, sql, repeat=args.repeat)
                print(f"{label}: min={minimum:.6f}s avg={avg:.6f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    args = parse_args()
    run_benchmarks(args)
