#!/usr/bin/env python3
"""Apply pending SQL migrations to the database.

Usage:
    DATABASE_URL=postgresql://... python db/migrate.py

Runs every *.sql file in db/migrations/ in lexicographic order.
Tracks applied migrations in a schema_migrations table so re-runs are safe.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def main() -> int:
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("Error: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("No migration files found.", file=sys.stderr)
        return 1

    with psycopg.connect(db_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(_BOOTSTRAP)
        conn.commit()

        for path in migration_files:
            filename = path.name
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM schema_migrations WHERE filename = %s", (filename,)
                )
                if cur.fetchone():
                    print(f"  skip  {filename} (already applied)")
                    continue

            sql = path.read_text(encoding="utf-8")
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (filename) VALUES (%s)", (filename,)
                    )
                conn.commit()
                print(f"  apply {filename}")
            except Exception as exc:
                conn.rollback()
                print(f"  ERROR {filename}: {exc}", file=sys.stderr)
                return 1

    print("Migrations complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
