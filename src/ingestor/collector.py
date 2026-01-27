from __future__ import annotations

import logging
import os
from pathlib import Path

import paramiko

from .config import AppSettings, SshServerConfig

logger = logging.getLogger(__name__)


def _ensure_input_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _collect_from_server(server: SshServerConfig, dest_dir: Path, username: str, password: str) -> int:
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

    logger.info("Connexion SSH à %s@%s", username, server.host)
    ssh.connect(server.host, username=username, password=password)

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

            remote_path = os.path.join(server.remote_dir, filename)
            local_path = server_dest_dir / filename

            if local_path.exists():
                logger.debug("Fichier déjà présent pour le serveur %s, on ignore: %s", server.name, local_path)
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
    total = 0

    if not settings.ssh_user or not settings.ssh_password:
        logger.error("SSH_USER ou SSH_PASSWORD non configuré dans l'environnement.")
        return 0

    for server in settings.ssh_servers:
        try:
            total += _collect_from_server(server, dest_dir, settings.ssh_user, settings.ssh_password)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors de la collecte depuis %s: %s", server.host, exc)

    logger.info("Collecte terminée, %d nouveau(x) fichier(s) téléchargé(s).", total)
    return total

