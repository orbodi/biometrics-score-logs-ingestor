import hashlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import List, Tuple

from .config import AppSettings
from .db import persist_records
from .parser import BiometricsRecord, FingerprintSample, parse_file
from .state import is_jsonl_already_persisted, mark_file_processed, mark_jsonl_persisted

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

    Version structurée pour les lignes RqType=IP.
    """
    if record.rq_type != "IP":
        # Pour l'instant on ne sérialise que les IP (filtrées en amont normalement)
        return {"rq_type": record.rq_type, "re_id": record.re_id}

    result: dict = {
        "rq_type": record.rq_type,
        "re_id": record.re_id,
        "re_code": record.status_code,
        "raw_line": record.raw,
    }

    # Face
    if (
        record.face_sample_id is not None
        or record.face_sample_type is not None
        or record.face_score is not None
    ):
        result["face"] = {
            "sample_id": record.face_sample_id,
            "sample_type": record.face_sample_type,
            "score": record.face_score,
        }

    # Iris
    if (
        record.iris_sample_id is not None
        or record.left_eye_score is not None
        or record.right_eye_score is not None
    ):
        result["iris"] = {
            "sample_id": record.iris_sample_id,
            "left": record.left_eye_score,
            "right": record.right_eye_score,
        }

    # Empreintes
    fingerprints_json = []
    for fp in record.fingerprint_samples:
        fingerprints_json.append(
            {
                "sample_id": fp.sample_id,
                "sample_type": fp.sample_type,
                "fingers": fp.values,
            }
        )
    if fingerprints_json:
        result["fingerprints"] = fingerprints_json

    # Extra éventuel (champs non traités explicitement)
    if record.extra:
        result["extra"] = record.extra

    return result


def _dict_to_record(data: dict) -> BiometricsRecord:
    """
    Convertit un dict JSON (tel que créé par _record_to_dict) en BiometricsRecord.
    Fonction inverse de _record_to_dict.
    """
    record = BiometricsRecord(
        rq_type=data.get("rq_type", ""),
        re_id=data.get("re_id", ""),
        status_code=data.get("re_code"),
        raw=data.get("raw_line"),
    )

    # Face
    if "face" in data:
        face_data = data["face"]
        record.face_sample_id = face_data.get("sample_id")
        record.face_sample_type = face_data.get("sample_type")
        record.face_score = face_data.get("score")

    # Iris
    if "iris" in data:
        iris_data = data["iris"]
        record.iris_sample_id = iris_data.get("sample_id")
        record.left_eye_score = iris_data.get("left")
        record.right_eye_score = iris_data.get("right")

    # Empreintes
    if "fingerprints" in data:
        for fp_data in data["fingerprints"]:
            fp_sample = FingerprintSample(
                sample_id=fp_data.get("sample_id", 0),
                sample_type=fp_data.get("sample_type"),
                values=fp_data.get("fingers", {}),
            )
            record.fingerprint_samples.append(fp_sample)

    # Extra
    if "extra" in data:
        record.extra = data["extra"]

    return record


def process_log_file(settings: AppSettings, log_path: Path) -> int:
    """
    Parse un fichier .log et écrit un .jsonl correspondant dans OUTPUT_JSON_DIR.
    Archive ensuite le .log traité dans ARCHIVE_DIR.

    - Ne garde que les lignes avec RqType=IP
    - Un record JSON par ligne dans un fichier .jsonl
    - Archive le .log après génération du JSONL

    Retourne le nombre de records IP écrits dans le JSONL.
    """
    records = parse_file(str(log_path))

    ip_records = [r for r in records if r.rq_type == "IP"]
    if not ip_records:
        logger.info("Aucun record RqType=IP dans %s, rien à exporter.", log_path)
        # On archive quand même le .log même s'il n'y a pas de records IP
        archive_log_file(settings, log_path)
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

    # Archive le .log après génération du JSONL
    archive_log_file(settings, log_path)

    return len(ip_records)


def archive_jsonl_file(settings: AppSettings, jsonl_path: Path) -> None:
    """
    Archive le fichier JSONL traité dans ARCHIVE_JSON_DIR en conservant la structure relative.
    """
    archive_base = Path(settings.archive_json_dir).resolve()
    output_base = Path(settings.output_json_dir).resolve()

    try:
        relative = jsonl_path.resolve().relative_to(output_base)
    except ValueError:
        # Si le fichier n'est pas sous OUTPUT_JSON_DIR, on le met à la racine d'ARCHIVE_JSON_DIR.
        relative = jsonl_path.name

    if isinstance(relative, Path):
        dest_path = archive_base / relative
    else:
        dest_path = archive_base / relative

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Archivage du JSONL %s vers %s...", jsonl_path, dest_path)
    shutil.move(str(jsonl_path), str(dest_path))
    logger.info("✓ Fichier JSONL archivé avec succès")


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
    logger.info("Archivage de %s vers %s...", log_path, dest_path)
    shutil.move(str(log_path), str(dest_path))
    logger.info("✓ Fichier archivé avec succès")

    # Marque le fichier comme traité dans le state SQLite.
    try:
        # On déduit le serveur à partir du premier segment du chemin relatif (INPUT_DIR/<server>/...)
        relative = log_path.resolve().relative_to(input_dir)
        server_name = relative.parts[0] if isinstance(relative, Path) else None
    except Exception:  # noqa: BLE001
        server_name = None

    filename = log_path.name

    # On tente de parser la date à partir du nom (ex: quality.YYYY-MM-DD.log)
    file_date = None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        file_date = m.group(1)

    # Calcul d'un hash sha256 pour info/diagnostic (sûr mais peut être coûteux sur de très gros fichiers)
    hash_sha256 = None
    try:
        h = hashlib.sha256()
        with dest_path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        hash_sha256 = h.hexdigest()
    except Exception:  # noqa: BLE001
        logger.exception("Impossible de calculer le hash sha256 pour %s", dest_path)

    if server_name:
        try:
            mark_file_processed(settings, server_name, filename, file_date=file_date, hash_sha256=hash_sha256)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Impossible de marquer le fichier %s/%s comme traité: %s", server_name, filename, exc)


def _iter_jsonl_files(output_dir: str) -> List[Path]:
    """
    Retourne la liste des fichiers .jsonl à archiver dans OUTPUT_JSON_DIR.
    """
    base = Path(output_dir)
    if not base.exists():
        return []

    return sorted(base.rglob("*.jsonl"))


def persist_all_jsonl_files(settings: AppSettings) -> Tuple[int, int]:
    """
    Persiste tous les fichiers JSONL présents dans OUTPUT_JSON_DIR en base de données,
    puis archive les JSONL vers ARCHIVE_JSON_DIR.

    - Lit et parse chaque JSONL
    - Persiste les données en base
    - Archive les JSONL après persistance réussie

    Retourne un tuple (nombre de fichiers traités, nombre total de lignes insérées en base).
    """
    jsonl_files = _iter_jsonl_files(settings.output_json_dir)
    if not jsonl_files:
        return (0, 0)

    processed_files = 0
    total_rows_inserted = 0

    for jsonl_path in jsonl_files:
        try:
            jsonl_path_str = str(jsonl_path)
            logger.info("Traitement du JSONL: %s", jsonl_path)

            # Vérifier si le JSONL a déjà été persisté
            if is_jsonl_already_persisted(settings, jsonl_path_str):
                logger.info("JSONL %s déjà persisté, archivage direct.", jsonl_path)
                archive_jsonl_file(settings, jsonl_path)
                processed_files += 1
                continue

            # Lire et parser le JSONL
            records: List[BiometricsRecord] = []
            with jsonl_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        record = _dict_to_record(data)
                        if record.rq_type == "IP":  # On ne garde que les IP
                            records.append(record)
                    except (json.JSONDecodeError, Exception) as exc:  # noqa: BLE001
                        logger.warning("Erreur lors du parsing d'une ligne JSON dans %s: %s", jsonl_path, exc)
                        continue

            if not records:
                logger.info("Aucun record RqType=IP dans %s, archivage direct.", jsonl_path)
                archive_jsonl_file(settings, jsonl_path)
                processed_files += 1
                continue

            # Persister en base
            try:
                # On déduit le serveur à partir du premier segment du chemin relatif
                output_base = Path(settings.output_json_dir).resolve()
                try:
                    relative = jsonl_path.resolve().relative_to(output_base)
                    server_name = relative.parts[0] if isinstance(relative, Path) and len(relative.parts) > 0 else None
                except Exception:  # noqa: BLE001
                    server_name = None

                source_file = jsonl_path.stem.replace(".log", "") + ".log"  # Retrouve le nom du .log original
                logger.info("Persistance en base de données pour %s...", source_file)
                persisted_rows = persist_records(settings, records, server_name=server_name, source_file=source_file)
                logger.info("✓ Persisté %d lignes de scores biométriques en base pour %s", persisted_rows, source_file)
                total_rows_inserted += persisted_rows

                # Marquer le JSONL comme persisté dans le state SQLite
                try:
                    mark_jsonl_persisted(
                        settings,
                        jsonl_path_str,
                        server_name=server_name,
                        source_file=source_file,
                        rows_inserted=persisted_rows,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Impossible de marquer le JSONL %s comme persisté dans le state: %s", jsonl_path, exc)

                # Archiver le JSONL après persistance réussie
                archive_jsonl_file(settings, jsonl_path)
                processed_files += 1

            except Exception as exc:  # noqa: BLE001
                logger.exception("Erreur lors de la persistance en base pour %s: %s", jsonl_path, exc)
                # On n'archive pas si la persistance a échoué

        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors du traitement du JSONL %s: %s", jsonl_path, exc)

    return (processed_files, total_rows_inserted)


def process_all_logs(settings: AppSettings) -> int:
    """
    Parse tous les fichiers .log présents dans INPUT_DIR :
    - Génère les JSONL correspondants (RqType=IP uniquement)
    - Archive les .log traités dans ARCHIVE_DIR

    Retourne le nombre de fichiers traités.
    """
    log_files = _iter_log_files(settings.input_dir)
    if not log_files:
        return 0

    processed_files = 0
    for log_path in log_files:
        try:
            process_log_file(settings, log_path)
            processed_files += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Erreur lors du traitement de %s: %s", log_path, exc)

    return processed_files

