import logging

from .collector import collect_from_servers
from .config import load_settings
from .cli import configure_logging


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger("ingestor.collect")

    logger.info("Démarrage de la collecte depuis les serveurs SSH configurés...")
    downloaded = collect_from_servers(settings)
    logger.info("Collecte terminée: %d fichier(s) téléchargé(s).", downloaded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

