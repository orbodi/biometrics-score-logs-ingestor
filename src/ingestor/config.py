import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv


@dataclass
class DatabaseSettings:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass
class AppSettings:
    log_level: str = "INFO"
    db: Optional[DatabaseSettings] = None
    input_dir: str = "inputs"
    output_json_dir: str = "outputs"
    ssh_servers: Optional[List["SshServerConfig"]] = None
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None
    ssh_timeout: int = 30


@dataclass
class SshServerConfig:
    name: str
    host: str
    remote_dir: str


def _parse_ssh_servers(raw: Optional[str]) -> List[SshServerConfig]:
    """
    Parse SSH_SERVERS depuis l'env.

    Format attendu (fallback) :
    host,remote_dir;host2,remote_dir2
    """
    servers: List[SshServerConfig] = []
    if not raw:
        return servers

    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = [p.strip() for p in entry.split(",")]
        if len(parts) != 2:
            # On ignore silencieusement les entrées mal formées pour le moment.
            continue
        host, remote_dir = parts
        servers.append(
            SshServerConfig(
                name=host,
                host=host,
                remote_dir=remote_dir,
            )
        )
    return servers


def _load_ssh_servers_from_file(path_str: Optional[str]) -> List[SshServerConfig]:
    """
    Charge la configuration des serveurs depuis un fichier JSON.

    Format attendu (liste d'objets) :
    [
      {
        "name": "server1",
        "host": "192.168.0.10",
        "remote_dir": "/var/log/biometrics"
      }
    ]
    """
    if not path_str:
        return []

    path = Path(path_str)

    # Si le chemin est relatif, on essaie d'abord par rapport au cwd,
    # puis par rapport à la racine du projet (3 niveaux au-dessus de ce fichier).
    if not path.is_absolute():
        if not path.exists():
            project_root = Path(__file__).resolve().parent.parent.parent
            path = project_root / path_str

    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    servers: List[SshServerConfig] = []
    for item in data:
        try:
            servers.append(
                SshServerConfig(
                    name=item["name"],
                    host=item["host"],
                    remote_dir=item["remote_dir"],
                )
            )
        except KeyError:
            # On ignore les entrées incomplètes
            continue
    return servers


def load_settings() -> AppSettings:
    """Charge la configuration à partir des variables d'environnement et du fichier .env."""
    # Charge le fichier .env s'il existe (une seule fois par process; inoffensif si rappelé)
    load_dotenv()
    db = DatabaseSettings(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "biometrics"),
        user=os.getenv("DB_USER", "biometrics_user"),
        password=os.getenv("DB_PASSWORD", "change_me"),
    )

    ssh_servers = _load_ssh_servers_from_file(os.getenv("SSH_SERVERS_FILE"))
    if not ssh_servers:
        # Fallback sur l'ancien format basé sur SSH_SERVERS
        ssh_servers = _parse_ssh_servers(os.getenv("SSH_SERVERS"))

    return AppSettings(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        db=db,
        input_dir=os.getenv("INPUT_DIR", "inputs"),
        output_json_dir=os.getenv("OUTPUT_JSON_DIR", "outputs"),
        ssh_servers=ssh_servers,
        ssh_user=os.getenv("SSH_USER"),
        ssh_password=os.getenv("SSH_PASSWORD"),
        ssh_timeout=int(os.getenv("SSH_TIMEOUT", "30")),
    )

