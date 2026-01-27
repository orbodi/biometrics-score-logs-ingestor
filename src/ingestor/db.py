from __future__ import annotations

import logging
from typing import Iterable

import psycopg2

from .config import AppSettings
from .parser import BiometricsRecord

logger = logging.getLogger(__name__)


def get_connection(settings: AppSettings):
    assert settings.db is not None, "Database settings must be configured"
    return psycopg2.connect(
        host=settings.db.host,
        port=settings.db.port,
        dbname=settings.db.name,
        user=settings.db.user,
        password=settings.db.password,
    )


def persist_records(settings: AppSettings, records: Iterable[BiometricsRecord]) -> int:
    """
    Persiste une liste de records en base.

    Implémentation volontairement simplifiée : à ce stade on illustre juste le flux,
    sans imposer un schéma précis de base de données.
    """
    inserted = 0
    conn = get_connection(settings)
    try:
        with conn, conn.cursor() as cur:
            for rec in records:
                # TODO: adapter au schéma réel (table, colonnes, etc.)
                cur.execute(
                    """
                    INSERT INTO biometrics_scores (rq_type, re_id, face_score, left_eye_score, right_eye_score, raw_line)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rec.rq_type,
                        rec.re_id,
                        rec.face_score,
                        rec.left_eye_score,
                        rec.right_eye_score,
                        rec.raw,
                    ),
                )
                inserted += 1
        logger.info("Persisted %s records into database", inserted)
    finally:
        conn.close()

    return inserted

