import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import AppSettings


def _get_db_path(settings: AppSettings) -> Path:
    """
    Retourne le chemin absolu vers le fichier SQLite de state.
    Par défaut : <project_root>/state/ingestor_state.db
    """
    raw = getattr(settings, "state_db_path", "state/ingestor_state.db")
    path = Path(raw)
    if not path.is_absolute():
        # project_root = src/ingestor/.. /..
        project_root = Path(__file__).resolve().parent.parent.parent
        path = project_root / raw
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_schema(conn: sqlite3.Connection) -> None:
    # Table pour les fichiers .log traités (copiés/parsés)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            server_name TEXT NOT NULL,
            filename TEXT NOT NULL,
            file_date TEXT,
            first_seen_at TEXT NOT NULL,
            last_processed_at TEXT NOT NULL,
            hash_sha256 TEXT,
            PRIMARY KEY (server_name, filename)
        )
        """
    )
    # Table pour les fichiers JSONL persistés en base
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS persisted_jsonl_files (
            jsonl_path TEXT NOT NULL PRIMARY KEY,
            server_name TEXT,
            source_file TEXT,
            rows_inserted INTEGER,
            first_persisted_at TEXT NOT NULL,
            last_persisted_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def is_file_already_processed(settings: AppSettings, server_name: str, filename: str) -> bool:
    """
    Retourne True si ce fichier (pour un serveur donné) a déjà été traité au moins une fois.
    """
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        cur = conn.execute(
            "SELECT 1 FROM processed_files WHERE server_name = ? AND filename = ? LIMIT 1",
            (server_name, filename),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_file_processed(
    settings: AppSettings,
    server_name: str,
    filename: str,
    file_date: Optional[str] = None,
    hash_sha256: Optional[str] = None,
) -> None:
    """
    Marque un fichier comme traité (copié + parsé) pour un serveur donné.
    Met à jour last_processed_at, conserve first_seen_at si déjà présent.
    """
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # On vérifie s'il existe déjà pour conserver first_seen_at
        cur = conn.execute(
            "SELECT first_seen_at FROM processed_files WHERE server_name = ? AND filename = ?",
            (server_name, filename),
        )
        row = cur.fetchone()
        first_seen_at = row[0] if row else now

        conn.execute(
            """
            INSERT INTO processed_files (server_name, filename, file_date, first_seen_at, last_processed_at, hash_sha256)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(server_name, filename) DO UPDATE SET
                file_date = excluded.file_date,
                last_processed_at = excluded.last_processed_at,
                hash_sha256 = excluded.hash_sha256
            """,
            (server_name, filename, file_date, first_seen_at, now, hash_sha256),
        )
        conn.commit()
    finally:
        conn.close()


def is_jsonl_already_persisted(settings: AppSettings, jsonl_path: str) -> bool:
    """
    Retourne True si ce fichier JSONL a déjà été persisté en base de données.
    """
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        cur = conn.execute(
            "SELECT 1 FROM persisted_jsonl_files WHERE jsonl_path = ? LIMIT 1",
            (jsonl_path,),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_jsonl_persisted(
    settings: AppSettings,
    jsonl_path: str,
    server_name: Optional[str] = None,
    source_file: Optional[str] = None,
    rows_inserted: Optional[int] = None,
) -> None:
    """
    Marque un fichier JSONL comme persisté en base de données.
    Met à jour last_persisted_at, conserve first_persisted_at si déjà présent.
    """
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # On vérifie s'il existe déjà pour conserver first_persisted_at
        cur = conn.execute(
            "SELECT first_persisted_at FROM persisted_jsonl_files WHERE jsonl_path = ?",
            (jsonl_path,),
        )
        row = cur.fetchone()
        first_persisted_at = row[0] if row else now

        conn.execute(
            """
            INSERT INTO persisted_jsonl_files (jsonl_path, server_name, source_file, rows_inserted, first_persisted_at, last_persisted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(jsonl_path) DO UPDATE SET
                server_name = excluded.server_name,
                source_file = excluded.source_file,
                rows_inserted = excluded.rows_inserted,
                last_persisted_at = excluded.last_persisted_at
            """,
            (jsonl_path, server_name, source_file, rows_inserted, first_persisted_at, now),
        )
        conn.commit()
    finally:
        conn.close()

