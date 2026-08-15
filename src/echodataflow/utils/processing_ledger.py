from __future__ import annotations

import sqlite3
from pathlib import Path


def initialize_ledger(db_path: str | Path) -> None:
    """Create the processing ledger database and required tables."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_sv (
                raw_path TEXT PRIMARY KEY,
                raw_filename TEXT NOT NULL,
                file_size INTEGER,
                file_mtime_ns INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                sv_filename TEXT,
                first_ping_time TEXT,
                last_ping_time TEXT,
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_sv_status
            ON raw_sv(status)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_sv_first_ping_time
            ON raw_sv(first_ping_time)
            """
        )


def register_raw_file(db_path: str | Path, raw_path: str | Path) -> None:
    """Register a RAW file, re-queueing it only when its contents changed."""
    raw_path = Path(raw_path)
    stat = raw_path.stat()

    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")

        existing = conn.execute(
            """
            SELECT file_size, file_mtime_ns
            FROM raw_sv
            WHERE raw_path = ?
            """,
            (str(raw_path),),
        ).fetchone()

        if existing is None:
            conn.execute(
                """
                INSERT INTO raw_sv (
                    raw_path,
                    raw_filename,
                    file_size,
                    file_mtime_ns,
                    status
                )
                VALUES (?, ?, ?, ?, 'pending')
                """,
                (
                    str(raw_path),
                    raw_path.name,
                    stat.st_size,
                    stat.st_mtime_ns,
                ),
            )
            return

        if existing != (stat.st_size, stat.st_mtime_ns):
            conn.execute(
                """
                UPDATE raw_sv
                SET file_size = ?,
                    file_mtime_ns = ?,
                    status = 'pending',
                    sv_filename = NULL,
                    first_ping_time = NULL,
                    last_ping_time = NULL,
                    error = '',
                    updated_at = CURRENT_TIMESTAMP
                WHERE raw_path = ?
                """,
                (
                    stat.st_size,
                    stat.st_mtime_ns,
                    str(raw_path),
                ),
            )

def get_raw_files_to_process(
    db_path: str | Path,
    limit: int = -1,
) -> list[Path]:
    """Return RAW files that are pending or failed."""
    query = """
        SELECT raw_path
        FROM raw_sv
        WHERE status IN ('pending', 'failed')
        ORDER BY created_at, raw_path
    """

    params: tuple = ()

    if limit != -1:
        query += " LIMIT ?"
        params = (limit,)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    return [Path(row[0]) for row in rows]

def mark_raw_processing(db_path: str | Path, raw_path: str | Path) -> None:
    """Mark a RAW file as currently being processed."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE raw_sv
            SET status = 'processing',
                error = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE raw_path = ?
            """,
            (str(Path(raw_path)),),
        )


def mark_raw_completed(
    db_path: str | Path,
    raw_path: str | Path,
    sv_filename: str,
    first_ping_time,
    last_ping_time,
) -> None:
    """Mark a RAW file as successfully converted to Sv."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE raw_sv
            SET status = 'completed',
                sv_filename = ?,
                first_ping_time = ?,
                last_ping_time = ?,
                error = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE raw_path = ?
            """,
            (
                sv_filename,
                str(first_ping_time),
                str(last_ping_time),
                str(Path(raw_path)),
            ),
        )


def mark_raw_failed(
    db_path: str | Path,
    raw_path: str | Path,
    error: str,
) -> None:
    """Mark a RAW file as failed."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE raw_sv
            SET status = 'failed',
                error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE raw_path = ?
            """,
            (error, str(Path(raw_path))),
        )

def get_completed_sv_files(
    db_path: str | Path,
    start_time=None,
    end_time=None,
) -> list[str]:
    """Return completed Sv files, optionally overlapping a time range."""
    query = """
        SELECT sv_filename
        FROM raw_sv
        WHERE status = 'completed'
          AND sv_filename IS NOT NULL
    """
    params = []

    if start_time is not None:
        query += """
          AND datetime(last_ping_time) >= datetime(?)
        """
        params.append(str(start_time))

    if end_time is not None:
        query += """
          AND datetime(first_ping_time) <= datetime(?)
        """
        params.append(str(end_time))

    query += """
        ORDER BY datetime(first_ping_time)
    """

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    return [row[0] for row in rows]