#!/usr/bin/env python3
"""
Minimal scaffold for Assignment 1:
- naive: INSERT per row
- batch: batched INSERT
- copy: PostgreSQL COPY FROM STDIN
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
from psycopg import sql


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load dblp/auth.tsv into PostgreSQL")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", default="postgres")
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="postgres")
    p.add_argument("--table", default="auth")
    p.add_argument("--tsv", default="dblp/auth.tsv")
    p.add_argument("--method", choices=["naive", "batch", "copy"], required=True)
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--limit", type=int, default=0, help="0 = all rows")
    p.add_argument("--truncate", action="store_true")
    return p.parse_args()


def connect(args: argparse.Namespace) -> psycopg.Connection:
    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        autocommit=False,
    )


def ensure_table(conn: psycopg.Connection, table: str) -> None:
    create_table_sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            name VARCHAR(49),
            pubid VARCHAR(129)
        )
        """
    ).format(table=sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()


def truncate_table(conn: psycopg.Connection, table: str) -> None:
    truncate_sql = sql.SQL("TRUNCATE TABLE {table}").format(table=sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(truncate_sql)
    conn.commit()


def iter_rows(tsv_path: Path, limit: int = 0) -> Iterator[tuple[str, str]]:
    with tsv_path.open("r", encoding="utf-8", newline="") as f:
        for i, line in enumerate(f, start=1):
            if limit and i > limit:
                break
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            yield parts[0], parts[1]


def load_naive(conn: psycopg.Connection, table: str, tsv_path: Path, limit: int) -> int:
    insert_sql = sql.SQL("INSERT INTO {table} (name, pubid) VALUES (%s, %s)").format(
        table=sql.Identifier(table)
    )
    count = 0
    with conn.cursor() as cur:
        for row in iter_rows(tsv_path, limit):
            cur.execute(insert_sql, row)
            count += 1
    conn.commit()
    return count


def load_batch(
    conn: psycopg.Connection, table: str, tsv_path: Path, limit: int, batch_size: int
) -> int:
    insert_sql = sql.SQL("INSERT INTO {table} (name, pubid) VALUES (%s, %s)").format(
        table=sql.Identifier(table)
    )
    count = 0
    batch: list[tuple[str, str]] = []
    with conn.cursor() as cur:
        for row in iter_rows(tsv_path, limit):
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            count += len(batch)
    conn.commit()
    return count


def load_copy(conn: psycopg.Connection, table: str, tsv_path: Path, limit: int) -> int:
    copy_sql = sql.SQL(
        "COPY {table} (name, pubid) "
        "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\b', ESCAPE E'\\b')"
    ).format(table=sql.Identifier(table))

    if limit == 0:
        with conn.cursor() as cur, tsv_path.open("r", encoding="utf-8", newline="") as f:
            with cur.copy(copy_sql) as copy:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    copy.write(chunk)
        conn.commit()
        # Fast line count after load for reporting
        count_sql = sql.SQL("SELECT COUNT(*) FROM {table}").format(table=sql.Identifier(table))
        with conn.cursor() as cur:
            cur.execute(count_sql)
            row = cur.fetchone()
            return int(row[0]) if row is not None else 0

    # Limited mode uses row-wise copy
    count = 0
    copy_limited_sql = sql.SQL("COPY {table} (name, pubid) FROM STDIN").format(
        table=sql.Identifier(table)
    )
    with conn.cursor() as cur:
        with cur.copy(copy_limited_sql) as copy:
            for row in iter_rows(tsv_path, limit):
                copy.write_row(row)
                count += 1
    conn.commit()
    return count


def main() -> None:
    args = parse_args()
    tsv_path = Path(args.tsv)
    if not tsv_path.exists():
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    with connect(args) as conn:
        ensure_table(conn, args.table)
        if args.truncate:
            truncate_table(conn, args.table)

        t0 = time.perf_counter()
        if args.method == "naive":
            inserted = load_naive(conn, args.table, tsv_path, args.limit)
        elif args.method == "batch":
            inserted = load_batch(conn, args.table, tsv_path, args.limit, args.batch_size)
        else:
            inserted = load_copy(conn, args.table, tsv_path, args.limit)
        t1 = time.perf_counter()

    elapsed = t1 - t0
    print(f"method={args.method} inserted={inserted} elapsed_s={elapsed:.3f}")


if __name__ == "__main__":
    main()
