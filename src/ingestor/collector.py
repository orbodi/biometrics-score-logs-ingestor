from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import paramiko

from .config import AppSettings, SshServerConfig
from .state import is_file_already_processed

logger = logging.getLogger(__name__)


def _ensure_input_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _extract_file_date(filename: str):
    """
    Extrait une date YYYY-MM-DD du nom de fichier, ou None si introuvable/incorrecte.
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _collect_from_server(
    server: SshServerConfig,
    dest_dir: Path,
    archive_dir: Path,
    username: str,
    password: str,
    timeout: int = 30,
) -> int:
    """
    Copie les fichiers .log depuis un serveur distant via SSH/SFTP.

    Règles :
    - On liste le répertoire `remote_dir`
    - On copie uniquement les fichiers se terminant par `.log`
    - On ne recopie pas un fichier déjà présent localement (même nom)
    """
    downloaded = 0

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    logger.info("Connexion SSH à %s@%s (timeout: %ds)", username, server.host, timeout)
    try:
        ssh.connect(server.host, username=username, password=password, timeout=timeout)
    except Exception as e:
        logger.error("Échec de la connexion SSH à %s@%s: %s", username, server.host, e)
        raise

    try:
        sftp = ssh.open_sftp()
        logger.info("Listing distant %s (%s)", server.remote_dir, server.name)

        # Répertoire local spécifique à ce serveur pour éviter les collisions de noms
        server_dest_dir = dest_dir / server.name
        server_dest_dir.mkdir(parents=True, exist_ok=True)

        for attr in sftp.listdir_attr(server.remote_dir):
            filename = attr.filename
            if not filename.lower().endswith(".log"):
                continue

            # Filtrage par date dans le nom du fichier (YYYY-MM-DD)
            file_date = _extract_file_date(filename)
            if not file_date:
                logger.debug(
                    "Nom de fichier sans date valide, on ignore pour le serveur %s: %s",
                    server.name,
                    filename,
                )
                continue
            if file_date > threshold:
                logger.debug(
                    "Fichier trop récent pour le serveur %s (date=%s > seuil=%s), on ignore: %s",
                    server.name,
                    file_date,
                    threshold,
                    filename,
                )
                continue

            remote_path = os.path.join(server.remote_dir, filename)
            local_path = server_dest_dir / filename

            # Vérifie si le fichier a déjà été traité selon le state SQLite.
            if is_file_already_processed(settings, server.name, filename):
                logger.debug(
                    "Fichier déjà marqué comme traité (state) pour le serveur %s, on ignore: %s",
                    server.name,
                    filename,
                )
                continue

            # Vérifie aussi la présence locale ou archivée (sécurité supplémentaire).
            archived_path = archive_dir / server.name / filename
            if local_path.exists() or archived_path.exists():
                logger.debug(
                    "Fichier déjà présent localement ou archivé pour le serveur %s, on ignore: %s (ou archivé: %s)",
                    server.name,
                    local_path,
                    archived_path,
                )
                continue

            logger.info("Téléchargement de %s vers %s", remote_path, local_path)
            sftp.get(remote_path, str(local_path))
            downloaded += 1
    finally:
        ssh.close()

    return downloaded


def collect_from_servers(settings: AppSettings) -> int:
    """
    Collecte les fichiers .log depuis tous les serveurs configurés.

    Retourne le nombre total de fichiers téléchargés.
    """
    if not settings.ssh_servers:
        logger.warning("Aucun serveur SSH configuré (SSH_SERVERS vide).")
        return 0

    dest_dir = _ensure_input_dir(settings.input_dir)
    archive_dir = Path(settings.archive_dir).resolve()

    # Seuil de date : on ne traite que les fichiers datés de la veille et avant.
    today = date.today()
    threshold = today - timedelta(days=1)
    total = 0

    if not settings.ssh_user or not settings.ssh_password:
        logger.error("SSH_USER ou SSH_PASSWORD non configuré dans l'environnement.")
        return 0

    for server in settings.ssh_servers:
        try:
            total += _collect_from_server(
                server,
                dest_dir,
                archive_dir,
                settings.ssh_user,
                settings.ssh_password,
                settings.ssh_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors de la collecte depuis %s (%s): %s", server.host, server.name, exc)

    logger.info("Collecte terminée, %d nouveau(x) fichier(s) téléchargé(s).", total)
    return total

