#!/usr/bin/env python3
"""
Load dblp/auth.tsv into MariaDB with three methods:
- naive: INSERT per row
- batch: batched INSERT
- load-data: MariaDB LOAD DATA LOCAL INFILE
"""

from __future__ import annotations

import argparse
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pymysql


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load dblp/auth.tsv into MariaDB")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--dbname", default="dbt_ex1")
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    p.add_argument("--table", default="auth")
    p.add_argument("--tsv", default="dblp/auth.tsv")
    p.add_argument("--method", choices=["naive", "batch", "load-data"], required=True)
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--limit", type=int, default=0, help="0 = all rows")
    p.add_argument("--truncate", action="store_true")
    return p.parse_args()


def connect(args: argparse.Namespace, *, with_db: bool) -> pymysql.connections.Connection:
    kwargs = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "autocommit": False,
        "local_infile": True,
    }
    if with_db:
        kwargs["database"] = args.dbname
    return pymysql.connect(**kwargs)


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def ensure_database(args: argparse.Namespace) -> None:
    dbname = quote_ident(args.dbname)
    with connect(args, with_db=False) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname} CHARACTER SET utf8mb4")
        conn.commit()


def ensure_table(conn: pymysql.connections.Connection, table: str) -> None:
    table_ident = quote_ident(table)
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_ident} (
            name VARCHAR(49),
            pubid VARCHAR(129)
        )
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()


def truncate_table(conn: pymysql.connections.Connection, table: str) -> None:
    table_ident = quote_ident(table)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_ident}")
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


def load_naive(conn: pymysql.connections.Connection, table: str, tsv_path: Path, limit: int) -> int:
    table_ident = quote_ident(table)
    insert_sql = f"INSERT INTO {table_ident} (name, pubid) VALUES (%s, %s)"
    count = 0
    with conn.cursor() as cur:
        for row in iter_rows(tsv_path, limit):
            cur.execute(insert_sql, row)
            count += 1
    conn.commit()
    return count


def load_batch(
    conn: pymysql.connections.Connection, table: str, tsv_path: Path, limit: int, batch_size: int
) -> int:
    table_ident = quote_ident(table)
    insert_sql = f"INSERT INTO {table_ident} (name, pubid) VALUES (%s, %s)"
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


def materialize_subset(tsv_path: Path, limit: int) -> Path:
    tmp = tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", suffix=".tsv", delete=False)
    tmp_path = Path(tmp.name)
    with tmp:
        for row in iter_rows(tsv_path, limit):
            tmp.write(f"{row[0]}\t{row[1]}\n")
    return tmp_path


def load_data(conn: pymysql.connections.Connection, table: str, tsv_path: Path, limit: int) -> int:
    table_ident = quote_ident(table)
    load_path = tsv_path
    tmp_path: Path | None = None
    try:
        if limit:
            tmp_path = materialize_subset(tsv_path, limit)
            load_path = tmp_path

        sql = (
            f"LOAD DATA LOCAL INFILE %s INTO TABLE {table_ident} "
            "FIELDS TERMINATED BY '\\t' "
            "LINES TERMINATED BY '\\n' "
            "(name, pubid)"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (str(load_path),))
            inserted = cur.rowcount
        conn.commit()
        return int(inserted)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def main() -> None:
    args = parse_args()
    tsv_path = Path(args.tsv)
    if not tsv_path.exists():
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    ensure_database(args)
    with connect(args, with_db=True) as conn:
        ensure_table(conn, args.table)
        if args.truncate:
            truncate_table(conn, args.table)

        t0 = time.perf_counter()
        if args.method == "naive":
            inserted = load_naive(conn, args.table, tsv_path, args.limit)
        elif args.method == "batch":
            inserted = load_batch(conn, args.table, tsv_path, args.limit, args.batch_size)
        else:
            inserted = load_data(conn, args.table, tsv_path, args.limit)
        t1 = time.perf_counter()

    elapsed = t1 - t0
    print(f"method={args.method} inserted={inserted} elapsed_s={elapsed:.3f}")


if __name__ == "__main__":
    main()
