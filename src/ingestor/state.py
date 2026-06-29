import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .config import AppSettings
from .disk_purge import with_disk_purge_retry
from .permissions import mkdir_p


def _get_db_path(settings: AppSettings) -> Path:
    """Retourne le chemin absolu vers le fichier SQLite de state."""
    return Path(settings.state_db_path)


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
    # Table pour les erreurs d'opération (persistance DB, etc.)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operation_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT NOT NULL,
            resource_key TEXT NOT NULL,
            server_name TEXT,
            source_file TEXT,
            error_type TEXT,
            error_message TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 1,
            first_failed_at TEXT NOT NULL,
            last_failed_at TEXT NOT NULL,
            resolved INTEGER NOT NULL DEFAULT 0,
            UNIQUE(operation, resource_key)
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


@dataclass
class OperationError:
    id: int
    operation: str
    resource_key: str
    server_name: Optional[str]
    source_file: Optional[str]
    error_type: Optional[str]
    error_message: str
    attempt_count: int
    first_failed_at: str
    last_failed_at: str


def record_operation_error(
    settings: AppSettings,
    operation: str,
    resource_key: str,
    exc: BaseException,
    *,
    server_name: Optional[str] = None,
    source_file: Optional[str] = None,
) -> None:
    """Enregistre ou met à jour une erreur d'opération dans le state SQLite."""
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        error_type = type(exc).__name__
        error_message = str(exc)

        cur = conn.execute(
            "SELECT id, attempt_count, first_failed_at FROM operation_errors "
            "WHERE operation = ? AND resource_key = ? AND resolved = 0",
            (operation, resource_key),
        )
        row = cur.fetchone()
        if row:
            attempt_count = row[1] + 1
            first_failed_at = row[2]
            conn.execute(
                """
                UPDATE operation_errors SET
                    server_name = ?,
                    source_file = ?,
                    error_type = ?,
                    error_message = ?,
                    attempt_count = ?,
                    last_failed_at = ?
                WHERE id = ?
                """,
                (
                    server_name,
                    source_file,
                    error_type,
                    error_message,
                    attempt_count,
                    now,
                    row[0],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO operation_errors (
                    operation, resource_key, server_name, source_file,
                    error_type, error_message, attempt_count,
                    first_failed_at, last_failed_at, resolved
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, 0)
                """,
                (
                    operation,
                    resource_key,
                    server_name,
                    source_file,
                    error_type,
                    error_message,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def resolve_operation_error(settings: AppSettings, operation: str, resource_key: str) -> None:
    """Marque une erreur comme résolue après succès de l'opération."""
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        conn.execute(
            "UPDATE operation_errors SET resolved = 1 WHERE operation = ? AND resource_key = ? AND resolved = 0",
            (operation, resource_key),
        )
        conn.commit()
    finally:
        conn.close()


def count_unresolved_errors(settings: AppSettings, operation: Optional[str] = None) -> int:
    """Compte les erreurs non résolues, optionnellement filtrées par type d'opération."""
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        if operation:
            cur = conn.execute(
                "SELECT COUNT(*) FROM operation_errors WHERE resolved = 0 AND operation = ?",
                (operation,),
            )
        else:
            cur = conn.execute("SELECT COUNT(*) FROM operation_errors WHERE resolved = 0")
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def list_unresolved_errors(
    settings: AppSettings,
    operation: Optional[str] = None,
) -> List[OperationError]:
    """Liste les erreurs non résolues."""
    db_path = _get_db_path(settings)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_schema(conn)
        if operation:
            cur = conn.execute(
                """
                SELECT id, operation, resource_key, server_name, source_file,
                       error_type, error_message, attempt_count,
                       first_failed_at, last_failed_at
                FROM operation_errors
                WHERE resolved = 0 AND operation = ?
                ORDER BY last_failed_at DESC
                """,
                (operation,),
            )
        else:
            cur = conn.execute(
                """
                SELECT id, operation, resource_key, server_name, source_file,
                       error_type, error_message, attempt_count,
                       first_failed_at, last_failed_at
                FROM operation_errors
                WHERE resolved = 0
                ORDER BY last_failed_at DESC
                """
            )
        return [
            OperationError(
                id=row[0],
                operation=row[1],
                resource_key=row[2],
                server_name=row[3],
                source_file=row[4],
                error_type=row[5],
                error_message=row[6],
                attempt_count=row[7],
                first_failed_at=row[8],
                last_failed_at=row[9],
            )
            for row in cur.fetchall()
        ]
    finally:
        conn.close()


def _resolve_error_storage_dir(settings: AppSettings) -> Path:
    return Path(settings.error_storage_dir)


def store_failed_resource(settings: AppSettings, source_path: Path, operation: str) -> Path:
    """
    Copie un fichier en échec vers ERROR_STORAGE_DIR/<operation>/ pour retraitement manuel.
    Retourne le chemin de destination.
    """
    storage_dir = mkdir_p(settings, _resolve_error_storage_dir(settings) / operation)

    dest = storage_dir / source_path.name
    if dest.exists():
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dest = storage_dir / f"{source_path.stem}_{ts}{source_path.suffix}"

    with_disk_purge_retry(settings, lambda: shutil.copy2(str(source_path), str(dest)))
    return dest

