import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple

from .collector import collect_from_servers
from .config import load_settings
from .cli import configure_logging
from .db import init_schema
from .disk_purge import purge_if_needed
from .permissions import mkdir_p
from .processor import persist_all_jsonl_files, process_all_logs
from .state import count_unresolved_errors, list_unresolved_errors, record_operation_error


def _configure_file_loggers(settings, execution_log_dir: str) -> Tuple[Path, Path]:
    """
    Configure deux fichiers de logs dans le dossier donné pour UNE exécution :
    - copie_logs_YYYYMMDD_HHMMSS.log  : phase de copie (collecte SSH)
    - parsing_logs_YYYYMMDD_HHMMSS.log: phase de parsing/JSON/archivage

    Retourne les chemins des deux fichiers de log.
    """
    log_dir = mkdir_p(settings, Path(execution_log_dir))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    copy_log_path = log_dir / f"copie_logs_{ts}.log"
    parsing_log_path = log_dir / f"parsing_logs_{ts}.log"

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # Handler pour la copie
    copy_handler = logging.FileHandler(copy_log_path, encoding="utf-8")
    copy_handler.setFormatter(formatter)

    # Handler pour le parsing
    parsing_handler = logging.FileHandler(parsing_log_path, encoding="utf-8")
    parsing_handler.setFormatter(formatter)

    # Loggers liés à la copie (collect CLI + collector SSH)
    for name in ["ingestor.collect", "ingestor.collector"]:
        logger = logging.getLogger(name)
        logger.addHandler(copy_handler)

    # Loggers liés au parsing / génération JSON / archivage
    for name in ["ingestor.processor"]:
        logger = logging.getLogger(name)
        logger.addHandler(parsing_handler)

    # Petits headers de début de run dans chaque fichier
    logging.getLogger("ingestor.collect").info("==== DÉBUT EXÉCUTION (copie) ====")
    logging.getLogger("ingestor.processor").info("==== DÉBUT EXÉCUTION (parsing) ====")

    return copy_log_path, parsing_log_path


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
    copy_log_path, parsing_log_path = _configure_file_loggers(settings, settings.execution_log_dir)

    logger = logging.getLogger("ingestor.collect")
    parsing_logger = logging.getLogger("ingestor.processor")

    purge_result = purge_if_needed(settings)
    purge_summary = ""
    if purge_result and purge_result.files_deleted > 0:
        purge_summary = (
            f"  Fichiers purgés (disque): {purge_result.files_deleted}\n"
            f"  Espace libéré: {purge_result.bytes_freed / (1024 * 1024):.1f} Mo\n"
        )
        logger.warning(
            "Purge disque au démarrage: %d fichier(s) supprimé(s), %.1f Mo libérés",
            purge_result.files_deleted,
            purge_result.bytes_freed / (1024 * 1024),
        )

    # Initialisation du schéma DB BI si INIT_DB=true dans .env
    if settings.db and settings.init_db:
        try:
            logger.info("Initialisation de la base de données BI (INIT_DB=true)...")
            init_schema(settings)
            logger.info("Base de données BI initialisée avec succès.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Impossible d'initialiser le schéma DB (persistance désactivée): %s", exc)
            try:
                record_operation_error(settings, "db_init", "schema", exc)
            except Exception:  # noqa: BLE001
                logger.exception("Impossible d'enregistrer l'erreur d'initialisation DB")
    elif settings.db:
        logger.info("INIT_DB=false, initialisation du schéma BI ignorée.")

    # 1) ÉTAPE COPIE : Collecte des nouveaux fichiers depuis les serveurs SSH
    logger.info("=== ÉTAPE 1: COPIE DES FICHIERS ===")
    logger.info("Démarrage de la collecte depuis les serveurs SSH configurés...")
    downloaded = collect_from_servers(settings)
    if downloaded == 0:
        logger.info("Aucun nouveau fichier à copier.")
    else:
        logger.info("Collecte terminée: %d fichier(s) téléchargé(s).", downloaded)

    # 2) ÉTAPE PARSING : Parsing des fichiers .log et génération des JSONL
    logger.info("=== ÉTAPE 2: PARSING DES FICHIERS .LOG ===")
    logger.info("Démarrage du parsing des fichiers .log dans %s...", settings.input_dir)
    processed_files = process_all_logs(settings)
    if processed_files == 0:
        logger.info("Aucun fichier .log à parser.")
    else:
        logger.info("Parsing terminé: %d fichier(s) parsé(s) et archivé(s).", processed_files)

    # 3) ÉTAPE PERSISTANCE : Persistance des JSONL en base puis archivage
    logger.info("=== ÉTAPE 3: PERSISTANCE ET ARCHIVAGE DES JSONL ===")
    logger.info("Démarrage de la persistance des JSONL dans %s...", settings.output_json_dir)
    processed_jsonl_files, total_rows_inserted, persist_failures = persist_all_jsonl_files(settings)
    if processed_jsonl_files == 0:
        logger.info("Aucun fichier JSONL à persister.")
    else:
        logger.info(
            "Persistance terminée: %d fichier(s) JSONL persisté(s) et archivé(s), %d lignes insérées.",
            processed_jsonl_files,
            total_rows_inserted,
        )
    if persist_failures > 0:
        parsing_logger.error(
            "%d échec(s) de persistance DB — fichiers conservés dans %s, copies dans %s",
            persist_failures,
            settings.output_json_dir,
            settings.error_storage_dir,
        )

    unresolved_errors = count_unresolved_errors(settings)
    if unresolved_errors > 0:
        parsing_logger.warning("%d erreur(s) non résolue(s) enregistrée(s) dans le state SQLite", unresolved_errors)
        for err in list_unresolved_errors(settings)[:5]:
            parsing_logger.warning(
                "  [%s] %s — %s (tentatives: %d)",
                err.operation,
                err.resource_key,
                err.error_message,
                err.attempt_count,
            )

    # 4) Résumé de l'exécution ajouté en fin de chaque fichier de log
    summary = (
        "=== RÉSUMÉ EXÉCUTION ===\n"
        f"{purge_summary}"
        f"  Fichiers copiés: {downloaded}\n"
        f"  Fichiers parsés: {processed_files}\n"
        f"  Fichiers JSONL persistés: {processed_jsonl_files}\n"
        f"  Lignes insérées en base: {total_rows_inserted}\n"
        f"  Échecs persistance DB: {persist_failures}\n"
        f"  Erreurs non résolues (state): {unresolved_errors}"
    )
    logger.info(summary)
    parsing_logger.info(summary)
    
    logger.info("=== EXÉCUTION TERMINÉE ===")
    parsing_logger.info("=== EXÉCUTION TERMINÉE ===")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

