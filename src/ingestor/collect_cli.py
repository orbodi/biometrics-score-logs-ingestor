import logging
from pathlib import Path

from .collector import collect_from_servers
from .config import load_settings
from .cli import configure_logging
from .processor import process_all_logs


def _configure_file_loggers(execution_log_dir: str) -> None:
    """
    Configure deux fichiers de logs dans le dossier donné :
    - copie_logs.log  : pour la phase de copie (collecte SSH)
    - parsing_logs.log: pour la phase de parsing/JSON/archivage
    """
    log_dir = Path(execution_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # Handler pour la copie
    copy_handler = logging.FileHandler(log_dir / "copie_logs.log", encoding="utf-8")
    copy_handler.setFormatter(formatter)

    # Handler pour le parsing
    parsing_handler = logging.FileHandler(log_dir / "parsing_logs.log", encoding="utf-8")
    parsing_handler.setFormatter(formatter)

    # Loggers liés à la copie (collect CLI + collector SSH)
    for name in ["ingestor.collect", "ingestor.collector"]:
        logger = logging.getLogger(name)
        logger.addHandler(copy_handler)

    # Loggers liés au parsing / génération JSON / archivage
    for name in ["ingestor.processor"]:
        logger = logging.getLogger(name)
        logger.addHandler(parsing_handler)


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
    _configure_file_loggers(settings.execution_log_dir)

    logger = logging.getLogger("ingestor.collect")

    # 1) Collecte des nouveaux fichiers depuis les serveurs SSH
    logger.info("Démarrage de la collecte depuis les serveurs SSH configurés...")
    downloaded = collect_from_servers(settings)
    logger.info("Collecte terminée: %d fichier(s) téléchargé(s).", downloaded)

    # 2) Parsing + génération JSON + archivage des .log présents dans INPUT_DIR
    logger.info("Démarrage du parsing des fichiers .log présents dans INPUT_DIR...")
    processed_files = process_all_logs(settings)
    if processed_files == 0:
        logger.info("Aucun fichier .log à parser, arrêt du programme.")
    else:
        logger.info("Parsing et archivage terminés pour %d fichier(s).", processed_files)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

