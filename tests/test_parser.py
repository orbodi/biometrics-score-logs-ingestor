from ingestor.parser import parse_line


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

