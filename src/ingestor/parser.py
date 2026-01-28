from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FingerprintSample:
    sample_id: int
    sample_type: Optional[str]
    # values[finger_name] = {"score": int | None, "nbpk": int | None}
    values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BiometricsRecord:
    rq_type: str
    re_id: str
    status_code: Optional[int] = None

    face_sample_id: Optional[int] = None
    face_sample_type: Optional[str] = None
    face_score: Optional[int] = None

    iris_sample_id: Optional[int] = None
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

    Cette version extrait :
    - RqType
    - premier ReId comme identifiant principal
    - ReId négatif (ex: -7, -5) comme code statut
    - bloc face (FaceSampleId, SampleType, Face)
    - bloc iris (IrisSampleId, LeftEye, RightEye)
    - plusieurs blocs empreintes (FingerprintSampleId, SampleType, doigts + nbpk)
    """
    tokens = [t for t in line.strip().split() if t]

    rq_type: str = ""
    primary_re_id: Optional[str] = None
    status_code: Optional[int] = None

    face_sample_id: Optional[int] = None
    face_sample_type: Optional[str] = None
    face_score: Optional[int] = None

    iris_sample_id: Optional[int] = None
    left_eye_score: Optional[int] = None
    right_eye_score: Optional[int] = None

    fingerprint_samples: List[FingerprintSample] = []
    current_fp: Optional[FingerprintSample] = None

    extra: Dict[str, Any] = {}

    finger_keys = {
        "RightThumb": "right_thumb",
        "RightIndex": "right_index",
        "RightMiddle": "right_middle",
        "RightRing": "right_ring",
        "RightLittle": "right_little",
        "LeftThumb": "left_thumb",
        "LeftIndex": "left_index",
        "LeftMiddle": "left_middle",
        "LeftRing": "left_ring",
        "LeftLittle": "left_little",
    }

    handled_keys = {
        "RqType",
        "ReId",
        "FaceSampleId",
        "SampleType",
        "Face",
        "IrisSampleId",
        "LeftEye",
        "RightEye",
        "FingerprintSampleId",
        "nbpk",
    }.union(finger_keys.keys())

    i = 0
    while i < len(tokens):
        token = tokens[i]
        if "=" not in token:
            i += 1
            continue
        key, value = token.split("=", 1)

        # RqType
        if key == "RqType":
            rq_type = value
            i += 1
            continue

        # ReId (principal + statut secondaire négatif)
        if key == "ReId":
            if primary_re_id is None:
                primary_re_id = value
            else:
                try:
                    code = int(value)
                    if code < 0:
                        status_code = code
                except ValueError:
                    pass
            i += 1
            continue

        # Face
        if key == "FaceSampleId":
            try:
                face_sample_id = int(value)
            except ValueError:
                pass
            i += 1
            continue

        if key == "SampleType" and current_fp is None and face_sample_type is None:
            # SampleType associé au visage (au début de la ligne)
            face_sample_type = value
            i += 1
            continue

        if key == "Face":
            try:
                face_score = int(value)
            except ValueError:
                pass
            i += 1
            continue

        # Iris
        if key == "IrisSampleId":
            try:
                iris_sample_id = int(value)
            except ValueError:
                pass
            i += 1
            continue

        if key == "LeftEye":
            try:
                left_eye_score = int(value)
            except ValueError:
                pass
            i += 1
            continue

        if key == "RightEye":
            try:
                right_eye_score = int(value)
            except ValueError:
                pass
            i += 1
            continue

        # Empreintes
        if key == "FingerprintSampleId":
            try:
                sample_id = int(value)
            except ValueError:
                sample_id = -1
            current_fp = FingerprintSample(sample_id=sample_id, sample_type=None, values={})
            fingerprint_samples.append(current_fp)
            i += 1
            continue

        if key == "SampleType" and current_fp is not None and current_fp.sample_type is None:
            current_fp.sample_type = value
            i += 1
            continue

        if key in finger_keys and current_fp is not None:
            finger_name = finger_keys[key]
            score: Optional[int]
            try:
                score = int(value)
            except ValueError:
                score = None

            nbpk_value: Optional[int] = None
            # On regarde le token suivant pour nbpk
            if i + 1 < len(tokens):
                next_token = tokens[i + 1]
                if "=" in next_token:
                    nkey, nvalue = next_token.split("=", 1)
                    if nkey == "nbpk":
                        try:
                            nbpk_value = int(nvalue)
                        except ValueError:
                            nbpk_value = None
                        # On consomme nbpk
                        i += 1

            current_fp.values[finger_name] = {"score": score, "nbpk": nbpk_value}
            i += 1
            continue

        # Extra : on stocke seulement les clés non gérées
        if key not in handled_keys:
            extra[key] = value

        i += 1

    record = BiometricsRecord(
        rq_type=rq_type,
        re_id=primary_re_id or "",
        status_code=status_code,
        face_sample_id=face_sample_id,
        face_sample_type=face_sample_type,
        face_score=face_score,
        iris_sample_id=iris_sample_id,
        left_eye_score=left_eye_score,
        right_eye_score=right_eye_score,
        raw=line.rstrip("\n"),
        extra=extra,
        fingerprint_samples=fingerprint_samples,
    )

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

