import argparse
import logging
import sys

from .config import load_settings
from .db import persist_records
from .parser import parse_file


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingestion des logs biométriques vers la base de données."
    )
    parser.add_argument(
        "log_file",
        help="Chemin vers le fichier de log à ingérer (ex: sample-data/quality.2026-01-26.log)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse et affiche un résumé sans persister en base.",
    )

    args = parser.parse_args(argv)

    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger("ingestor.cli")

    logger.info("Parsing log file %s", args.log_file)
    records = parse_file(args.log_file)
    logger.info("Parsed %d records", len(records))

    if args.dry_run:
        # On affiche juste quelques exemples pour validation.
        for rec in records[:5]:
            logger.info(
                "Record rq_type=%s re_id=%s face=%s left_eye=%s right_eye=%s",
                rec.rq_type,
                rec.re_id,
                rec.face_score,
                rec.left_eye_score,
                rec.right_eye_score,
            )
        logger.info("Dry-run terminé, aucune donnée persistée.")
        return 0

    logger.info("Persisting records into database...")
    inserted = persist_records(settings, records)
    logger.info("Done. %d records inserted.", inserted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

