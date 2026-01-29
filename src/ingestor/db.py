import logging
import re
from datetime import date
from typing import Iterable, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .config import AppSettings
from .models import Base, BiometricScore, configure_schema
from .parser import BiometricsRecord

logger = logging.getLogger(__name__)

# Mapping des noms de doigts du parser vers les channels SQL
FINGER_CHANNEL_MAP = {
    "right_thumb": "RIGHT_THUMB",
    "right_index": "RIGHT_INDEX",
    "right_middle": "RIGHT_MIDDLE",
    "right_ring": "RIGHT_RING",
    "right_little": "RIGHT_LITTLE",
    "left_thumb": "LEFT_THUMB",
    "left_index": "LEFT_INDEX",
    "left_middle": "LEFT_MIDDLE",
    "left_ring": "LEFT_RING",
    "left_little": "LEFT_LITTLE",
}


def get_engine(settings: AppSettings):
    """Crée et retourne un engine SQLAlchemy."""
    assert settings.db is not None, "Database settings must be configured"
    connection_string = (
        f"postgresql://{settings.db.user}:{settings.db.password}@"
        f"{settings.db.host}:{settings.db.port}/{settings.db.name}"
    )
    return create_engine(connection_string, echo=False)


def get_session(settings: AppSettings) -> Session:
    """Crée et retourne une session SQLAlchemy."""
    engine = get_engine(settings)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def init_schema(settings: AppSettings) -> None:
    """
    Initialise la base de données :
    1. Crée le schéma PostgreSQL s'il n'existe pas
    2. Crée les tables si elles n'existent pas (basé sur les modèles SQLAlchemy).
    
    Configure le schéma depuis les settings avant création.
    """
    assert settings.db is not None, "Database settings must be configured"
    schema = settings.db.schema
    
    # Validation du nom de schéma (sécurité : éviter l'injection SQL)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema):
        raise ValueError(f"Invalid schema name: '{schema}'. Must be a valid PostgreSQL identifier.")
    
    engine = get_engine(settings)
    
    # Créer le schéma s'il n'existe pas (sauf pour 'public' qui existe toujours)
    if schema != "public":
        with engine.connect() as conn:
            # Utiliser text() pour exécuter du SQL brut
            # Note: on valide le nom du schéma ci-dessus pour éviter l'injection SQL
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()
        logger.info("Schema '%s' created if needed", schema)
    
    # Configurer le schéma pour les modèles et créer les tables
    configure_schema(schema)
    Base.metadata.create_all(engine)
    logger.info("Database initialized (tables created if needed in schema '%s')", schema)


def _extract_date_from_filename(filename: Optional[str]) -> Optional[date]:
    """Extrait une date YYYY-MM-DD du nom de fichier."""
    if not filename:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def _record_to_biometric_scores(
    record: BiometricsRecord,
    server_name: Optional[str] = None,
    source_file: Optional[str] = None,
) -> List[BiometricScore]:
    """
    Transforme un BiometricsRecord (RqType=IP) en plusieurs lignes BiometricScore.

    Retourne une liste d'objets BiometricScore prêts à être insérés :
    - 1 pour le visage (si présent)
    - 2 pour l'iris (gauche + droite, si présents)
    - N×10 pour les empreintes (par sample_id et par doigt)
    """
    scores: List[BiometricScore] = []
    log_date = _extract_date_from_filename(source_file)

    # Face
    if record.face_score is not None:
        scores.append(
            BiometricScore(
                re_id=record.re_id,
                re_code=record.status_code,
                rq_type=record.rq_type,
                log_date=log_date,
                server_name=server_name,
                source_file=source_file,
                modality="FACE",
                channel="FACE",
                sample_id=record.face_sample_id,
                sample_type=record.face_sample_type,
                score=record.face_score,
                nbpk=None,
                raw_line=record.raw,
            )
        )

    # Iris (2 lignes : gauche + droite)
    if record.left_eye_score is not None:
        scores.append(
            BiometricScore(
                re_id=record.re_id,
                re_code=record.status_code,
                rq_type=record.rq_type,
                log_date=log_date,
                server_name=server_name,
                source_file=source_file,
                modality="IRIS",
                channel="LEFT_EYE",
                sample_id=record.iris_sample_id,
                sample_type=None,
                score=record.left_eye_score,
                nbpk=None,
                raw_line=record.raw,
            )
        )

    if record.right_eye_score is not None:
        scores.append(
            BiometricScore(
                re_id=record.re_id,
                re_code=record.status_code,
                rq_type=record.rq_type,
                log_date=log_date,
                server_name=server_name,
                source_file=source_file,
                modality="IRIS",
                channel="RIGHT_EYE",
                sample_id=record.iris_sample_id,
                sample_type=None,
                score=record.right_eye_score,
                nbpk=None,
                raw_line=record.raw,
            )
        )

    # Empreintes (par sample_id et par doigt)
    for fp_sample in record.fingerprint_samples:
        for finger_name, finger_data in fp_sample.values.items():
            if not isinstance(finger_data, dict):
                continue
            score_val = finger_data.get("score")
            nbpk_val = finger_data.get("nbpk")

            channel = FINGER_CHANNEL_MAP.get(finger_name)
            if not channel:
                # On skip les doigts non mappés
                continue

            scores.append(
                BiometricScore(
                    re_id=record.re_id,
                    re_code=record.status_code,
                    rq_type=record.rq_type,
                    log_date=log_date,
                    server_name=server_name,
                    source_file=source_file,
                    modality="FINGER",
                    channel=channel,
                    sample_id=fp_sample.sample_id,
                    sample_type=fp_sample.sample_type,
                    score=score_val,
                    nbpk=nbpk_val,
                    raw_line=record.raw,
                )
            )

    return scores


def persist_records(
    settings: AppSettings,
    records: Iterable[BiometricsRecord],
    server_name: Optional[str] = None,
    source_file: Optional[str] = None,
) -> int:
    """
    Persiste une liste de records IP en base via SQLAlchemy.

    Transforme chaque BiometricsRecord en plusieurs lignes BiometricScore (face, iris, doigts)
    et les insère en bulk pour performance.
    """
    session = get_session(settings)
    total_inserted = 0

    try:
        all_scores: List[BiometricScore] = []
        for rec in records:
            if rec.rq_type != "IP":
                # On ne persiste que les IP pour l'instant
                continue
            all_scores.extend(_record_to_biometric_scores(rec, server_name, source_file))

        if all_scores:
            session.bulk_save_objects(all_scores)
            session.commit()
            total_inserted = len(all_scores)
            logger.info("Persisted %d biometric score rows into database", total_inserted)
        else:
            logger.info("No IP records to persist")
    except Exception as exc:
        session.rollback()
        logger.exception("Error persisting records: %s", exc)
        raise
    finally:
        session.close()

    return total_inserted

