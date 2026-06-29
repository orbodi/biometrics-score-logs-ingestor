from ingestor.collector import _is_log_file


def test_is_log_file():
    assert _is_log_file("quality.2026-06-29.log")
    assert _is_log_file("QUALITY.LOG")
    assert not _is_log_file("quality.2026-06-29.json")
    assert not _is_log_file("readme.txt")
