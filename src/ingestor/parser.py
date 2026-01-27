from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FingerprintSample:
    sample_id: int
    sample_type: Optional[str]
    values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BiometricsRecord:
    rq_type: str
    re_id: str
    face_score: Optional[int] = None
    left_eye_score: Optional[int] = None
    right_eye_score: Optional[int] = None
    raw: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    fingerprint_samples: List[FingerprintSample] = field(default_factory=list)


def parse_line(line: str) -> BiometricsRecord:
    """
    Parse une ligne de log brute en structure Python.

    Le format est de type :
    RqType=IP ReId=438326870647742011 FaceSampleId=1 SampleType=STILL Face=200 IrisSampleId=1 LeftEye=84 RightEye=84 ...

    Cette première version reste volontairement simple :
    - extrait les champs principaux (RqType, ReId, Face, LeftEye, RightEye)
    - stocke tout le reste dans `extra`
    - ne gère pas encore finement les multiples FingerprintSampleId (à raffiner si besoin)
    """
    tokens = [t for t in line.strip().split() if t]
    kv: Dict[str, Any] = {}

    for token in tokens:
        if "=" in token:
            key, value = token.split("=", 1)
            kv[key] = value

    rq_type = kv.get("RqType", "")
    re_id = kv.get("ReId", "")

    record = BiometricsRecord(
        rq_type=rq_type,
        re_id=re_id,
        face_score=int(kv["Face"]) if "Face" in kv else None,
        left_eye_score=int(kv["LeftEye"]) if "LeftEye" in kv else None,
        right_eye_score=int(kv["RightEye"]) if "RightEye" in kv else None,
        raw=line.rstrip("\n"),
    )

    # Extra : tout ce qui n'est pas déjà mappé explicitement
    for k, v in kv.items():
        if k in {"RqType", "ReId", "Face", "LeftEye", "RightEye"}:
            continue
        record.extra[k] = v

    return record


def parse_file(path: str) -> List[BiometricsRecord]:
    """Parse un fichier de logs complet et renvoie une liste de records."""
    records: List[BiometricsRecord] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(parse_line(line))
    return records

