from datetime import date, datetime
from typing import Optional

from sqlalchemy import Column, Date, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


def configure_schema(schema: str) -> None:
    """
    Configure le schéma pour le modèle BiometricScore.
    À appeler depuis db.py avant init_schema().
    Force la création de __table__ si nécessaire, puis modifie le schéma.
    """
    # Force la création de __table__ si pas encore fait
    if not hasattr(BiometricScore, "__table__"):
        # Accéder à __table__ force SQLAlchemy à le créer
        _ = BiometricScore.__table__
    # Modifier le schéma directement sur l'objet Table
    BiometricScore.__table__.schema = schema


class BiometricScore(Base):
    """
    Modèle SQLAlchemy pour la table biometric_scores (fact table pour le data mart).

    Chaque ligne représente un score biométrique unique :
    - 1 ligne pour le visage (modality='FACE', channel='FACE')
    - 2 lignes pour l'iris (modality='IRIS', channel='LEFT_EYE' / 'RIGHT_EYE')
    - N×10 lignes pour les empreintes (modality='FINGER', channel='RIGHT_THUMB', ...)
    """

    __tablename__ = "biometric_scores"
    __table_args__ = {"schema": "public"}  # Par défaut, sera surchargé par configure_schema()

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Contexte transaction
    re_id = Column(String, nullable=False, index=True)
    re_code = Column(Integer, nullable=True)
    rq_type = Column(String, nullable=False, default="IP")
    log_date = Column(Date, nullable=True, index=True)
    server_name = Column(String, nullable=True, index=True)
    source_file = Column(String, nullable=True)

    # Dimensions biométriques
    modality = Column(String, nullable=False, index=True)  # 'FACE' | 'IRIS' | 'FINGER'
    channel = Column(String, nullable=False, index=True)  # 'FACE' | 'LEFT_EYE' | 'RIGHT_EYE' | 'RIGHT_THUMB' | ...
    sample_id = Column(Integer, nullable=True)  # FaceSampleId / IrisSampleId / FingerprintSampleId
    sample_type = Column(String, nullable=True)  # STILL, TENPRINT_SLAP, ...

    # Mesures
    score = Column(Integer, nullable=True)
    nbpk = Column(Integer, nullable=True)  # Nombre de points caractéristiques (minutiae) - NULL pour FACE/IRIS

    # Technique
    raw_line = Column(Text, nullable=True)
    created_at = Column(
        "created_at",
        type_=datetime,
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self):
        return (
            f"<BiometricScore(id={self.id}, re_id={self.re_id}, "
            f"modality={self.modality}, channel={self.channel}, score={self.score})>"
        )
