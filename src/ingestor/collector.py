from __future__ import annotations

import logging
import os
from pathlib import Path

from .config import AppSettings, SshServerConfig
from .disk_purge import with_disk_purge_retry
from .permissions import chmod_path, configure_ssh_client, mkdir_p
from .state import mark_file_downloaded, should_skip_file_copy

logger = logging.getLogger(__name__)


def _ensure_input_dir(settings: AppSettings, path: str) -> Path:
    return mkdir_p(settings, Path(path))


def _is_log_file(filename: str) -> bool:
    """Indique si le fichier distant doit être collecté."""
    return filename.lower().endswith(".log")


def _collect_from_server(
    settings: AppSettings,
    server: SshServerConfig,
    dest_dir: Path,
    username: str,
    password: str,
    timeout: int,
) -> int:
    """
    Copie les fichiers .log depuis un serveur distant via SSH/SFTP.

    Règles :
    - On liste le répertoire `remote_dir`
    - On copie tous les fichiers se terminant par `.log`
    - On ne recopie pas un fichier déjà présent localement (même nom)
    """
    downloaded = 0

    ssh = configure_ssh_client(settings)

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
        server_dest_dir = mkdir_p(settings, dest_dir / server.name)

        for attr in sftp.listdir_attr(server.remote_dir):
            filename = attr.filename
            if not _is_log_file(filename):
                continue

            remote_path = os.path.join(server.remote_dir, filename)
            local_path = server_dest_dir / filename

            skip, reason = should_skip_file_copy(settings, server.name, filename)
            if skip:
                logger.info(
                    "Copie ignorée pour %s/%s : %s",
                    server.name,
                    filename,
                    reason,
                )
                continue

            logger.info("Téléchargement de %s vers %s", remote_path, local_path)
            with_disk_purge_retry(settings, lambda: sftp.get(remote_path, str(local_path)))
            chmod_path(settings, local_path, is_dir=False)
            mark_file_downloaded(
                settings,
                server.name,
                filename,
                file_size=local_path.stat().st_size,
            )
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

    dest_dir = _ensure_input_dir(settings, settings.input_dir)
    total = 0

    logger.info("Collecte de tous les fichiers .log disponibles dans les dossiers distants.")

    if not settings.ssh_user or not settings.ssh_password:
        logger.error("SSH_USER ou SSH_PASSWORD non configuré dans l'environnement.")
        return 0

    for server in settings.ssh_servers:
        try:
            total += _collect_from_server(
                settings,
                server,
                dest_dir,
                settings.ssh_user,
                settings.ssh_password,
                settings.ssh_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors de la collecte depuis %s (%s): %s", server.host, server.name, exc)

    logger.info("Collecte terminée, %d nouveau(x) fichier(s) téléchargé(s).", total)
    return total

