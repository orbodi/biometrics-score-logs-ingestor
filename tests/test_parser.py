import gzip
import tempfile
from pathlib import Path

from ingestor.parser import parse_file, parse_line, summarize_rq_types

SAMPLE_IP_FULL = (
    "RqType=IP ReId=7569797491353854 FaceSampleId=1 SampleType=STILL Face=199 "
    "IrisSampleId=1 LeftEye=82 RightEye=83 ReId=-7 FingerprintSampleId=0 "
    "SampleType=TENPRINT_SLAP RightThumb=140 nbpk=56 RightIndex=91 nbpk=45 "
    "RightMiddle=107 nbpk=40 RightRing=104 nbpk=54 RightLittle=66 nbpk=33 "
    "LeftThumb=155 nbpk=68 LeftIndex=82 nbpk=46 LeftMiddle=116 nbpk=46 "
    "LeftRing=111 nbpk=60 LeftLittle=66 nbpk=33"
)

SAMPLE_IP_FACE_ONLY = (
    "RqType=IP ReId=7570131817595919 FaceSampleId=1 SampleType=STILL Face=231"
)

SAMPLE_MATCHPP = (
    "RqType=MATCHPP ReId=25663981028354258 IrisSampleId=1 LeftEye=80 RightEye=86 "
    "ReId=-5 FingerprintSampleId=0 SampleType=TENPRINT_SLAP RightThumb=186 nbpk=53"
)


def test_parse_line_basic():
    line = (
        "RqType=IP ReId=438326870647742011 FaceSampleId=1 SampleType=STILL "
        "Face=200 IrisSampleId=1 LeftEye=84 RightEye=84"
    )
    rec = parse_line(line)

    assert rec.rq_type == "IP"
    assert rec.re_id == "438326870647742011"
    assert rec.face_score == 200
    assert rec.left_eye_score == 84
    assert rec.right_eye_score == 84


def test_parse_line_production_sample_full_ip():
    rec = parse_line(SAMPLE_IP_FULL)

    assert rec.rq_type == "IP"
    assert rec.re_id == "7569797491353854"
    assert rec.status_code == -7
    assert rec.face_score == 199
    assert rec.left_eye_score == 82
    assert rec.right_eye_score == 83
    assert rec.fingerprint_samples[0].values["right_thumb"] == {"score": 140, "nbpk": 56}
    assert rec.fingerprint_samples[0].values["left_little"] == {"score": 66, "nbpk": 33}


def test_parse_line_production_sample_face_only():
    rec = parse_line(SAMPLE_IP_FACE_ONLY)
    assert rec.rq_type == "IP"
    assert rec.face_score == 231
    assert rec.fingerprint_samples == []


def test_parse_file_filters_rq_types():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "quality.log"
        path.write_text(
            "\n".join([SAMPLE_IP_FULL, SAMPLE_MATCHPP, SAMPLE_IP_FACE_ONLY]),
            encoding="utf-8",
        )
        records = parse_file(str(path))
        summary = summarize_rq_types(records)

    assert summary["IP"] == 2
    assert summary["MATCHPP"] == 1


def test_parse_file_utf8_bom():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "quality.log"
        path.write_bytes("\ufeff".encode("utf-8") + SAMPLE_IP_FACE_ONLY.encode("utf-8"))
        records = parse_file(str(path))

    assert len(records) == 1
    assert records[0].rq_type == "IP"


def test_parse_file_gzip():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "quality.log"
        content = "\n".join([SAMPLE_IP_FULL, SAMPLE_MATCHPP]).encode("utf-8")
        path.write_bytes(gzip.compress(content))
        records = parse_file(str(path))

    assert summarize_rq_types(records) == {"IP": 1, "MATCHPP": 1}
