import json
import logging
import shutil
from pathlib import Path
from typing import List

from .config import AppSettings
from .parser import BiometricsRecord, parse_file

logger = logging.getLogger(__name__)


def _iter_log_files(input_dir: str) -> List[Path]:
    """
    Retourne la liste des fichiers .log à traiter dans INPUT_DIR.

    On cherche récursivement, ce qui couvre le cas INPUT_DIR/<serveur>/*.log.
    """
    base = Path(input_dir)
    if not base.exists():
        return []

    return sorted(base.rglob("*.log"))


def _record_to_dict(record: BiometricsRecord) -> dict:
    """
    Convertit un BiometricsRecord en dict prêt à être sérialisé en JSON.

    Version simple : on aplatit les champs principaux + `extra`.
    """
    data = {
        "rq_type": record.rq_type,
        "re_id": record.re_id,
        "face_score": record.face_score,
        "left_eye_score": record.left_eye_score,
        "right_eye_score": record.right_eye_score,
    }
    data.update(record.extra)
    return data


def process_log_file(settings: AppSettings, log_path: Path) -> int:
    """
    Parse un fichier .log et écrit un .jsonl correspondant dans OUTPUT_JSON_DIR.

    - Ne garde que les lignes avec RqType=IP
    - Un record JSON par ligne dans un fichier .jsonl
    - Archive ensuite le .log traité dans ARCHIVE_DIR (géré par le call site)

    Retourne le nombre de records IP écrits.
    """
    records = parse_file(str(log_path))

    ip_records = [r for r in records if r.rq_type == "IP"]
    if not ip_records:
        logger.info("Aucun record RqType=IP dans %s, rien à exporter.", log_path)
        return 0

    # On met les JSON dans OUTPUT_JSON_DIR, en gardant la structure de dossiers.
    input_dir = Path(settings.input_dir).resolve()
    output_base = Path(settings.output_json_dir).resolve()

    try:
        relative = log_path.resolve().relative_to(input_dir)
    except ValueError:
        # Si pour une raison quelconque le fichier ne se trouve pas sous INPUT_DIR,
        # on le met directement à la racine d'OUTPUT_JSON_DIR avec son nom.
        relative = log_path.name

    # On remplace l'extension par .jsonl
    if isinstance(relative, Path):
        output_path = output_base / relative
    else:
        output_path = output_base / relative
    output_path = output_path.with_suffix(output_path.suffix + ".jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Écriture du JSONL de %s vers %s", log_path, output_path)
    with output_path.open("w", encoding="utf-8") as f:
        for rec in ip_records:
            json.dump(_record_to_dict(rec), f, ensure_ascii=False)
            f.write("\n")

    return len(ip_records)


def archive_log_file(settings: AppSettings, log_path: Path) -> None:
    """
    Archive le fichier .log traité dans ARCHIVE_DIR en conservant la structure relative.
    """
    archive_base = Path(settings.archive_dir).resolve()
    input_dir = Path(settings.input_dir).resolve()

    try:
        relative = log_path.resolve().relative_to(input_dir)
    except ValueError:
        # Si le fichier n'est pas sous INPUT_DIR, on le met à la racine d'ARCHIVE_DIR.
        relative = log_path.name

    if isinstance(relative, Path):
        dest_path = archive_base / relative
    else:
        dest_path = archive_base / relative

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Archivage de %s vers %s", log_path, dest_path)
    shutil.move(str(log_path), str(dest_path))


def process_all_logs(settings: AppSettings) -> int:
    """
    Traite tous les fichiers .log présents dans INPUT_DIR :
    - Génère les JSONL correspondants (RqType=IP uniquement)
    - Archive les .log traités dans ARCHIVE_DIR

    Retourne le nombre total de fichiers .log traités.
    """
    log_files = _iter_log_files(settings.input_dir)
    if not log_files:
        return 0

    processed_files = 0
    for log_path in log_files:
        try:
            written = process_log_file(settings, log_path)
            if written > 0:
                archive_log_file(settings, log_path)
            else:
                # Si aucun record IP, on peut choisir d'archiver quand même ou de laisser en place.
                archive_log_file(settings, log_path)
            processed_files += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors du traitement de %s: %s", log_path, exc)

    return processed_files

