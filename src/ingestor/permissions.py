import logging
import os
import re
from pathlib import Path

import paramiko
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import AppSettings

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def chmod_path(settings: AppSettings, path: Path, *, is_dir: bool) -> None:
    if not settings.auto_grant_permissions:
        return
    if not path.exists():
        return
    mode = settings.dir_permission_mode if is_dir else settings.file_permission_mode
    try:
        os.chmod(path, mode)
    except OSError as exc:
        logger.warning("Impossible d'appliquer les permissions sur %s: %s", path, exc)


def mkdir_p(settings: AppSettings, path: Path) -> Path:
    """Crée un répertoire (parents inclus) et applique les permissions configurées."""
    path.mkdir(parents=True, exist_ok=True)
    chmod_path(settings, path, is_dir=True)
    return path


def apply_workspace_permissions(settings: AppSettings) -> None:
    """Applique les permissions sur ROOT_DIR et tous les sous-dossiers connus."""
    if not settings.auto_grant_permissions:
        return
    paths = [
        settings.root_dir,
        settings.input_dir,
        settings.output_json_dir,
        settings.archive_dir,
        settings.archive_json_dir,
        settings.execution_log_dir,
        settings.error_storage_dir,
        str(Path(settings.state_db_path).parent),
    ]
    seen: set[str] = set()
    for raw in paths:
        if raw in seen:
            continue
        seen.add(raw)
        path = Path(raw)
        if path.exists():
            chmod_path(settings, path, is_dir=path.is_dir())


def configure_ssh_client(settings: AppSettings) -> paramiko.SSHClient:
    """Configure le client SSH (acceptation auto des clés hôtes si activée)."""
    ssh = paramiko.SSHClient()
    if settings.ssh_auto_accept_host_key:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.debug("SSH: acceptation automatique des clés hôtes activée")
    else:
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    return ssh


def grant_db_permissions(settings: AppSettings, engine: Engine) -> None:
    """Accorde les droits PostgreSQL nécessaires à l'utilisateur applicatif."""
    if not settings.auto_grant_permissions:
        return

    assert settings.db is not None
    schema = settings.db.schema
    user = settings.db.user

    if not _IDENTIFIER_RE.match(schema):
        raise ValueError(f"Invalid schema name: '{schema}'")
    if not _IDENTIFIER_RE.match(user):
        raise ValueError(f"Invalid database user: '{user}'")

    statements = [
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{user}"',
        f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" TO "{user}"',
        f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{user}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON TABLES TO "{user}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON SEQUENCES TO "{user}"',
    ]

    with engine.connect() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()

    logger.info("Droits PostgreSQL accordés à '%s' sur le schéma '%s'", user, schema)
