import logging

from .collector import collect_from_servers
from .config import load_settings
from .cli import configure_logging
from .processor import process_all_logs


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
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

