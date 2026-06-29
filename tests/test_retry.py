from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError

from ingestor.retry import is_transient_db_error, with_retry


def test_is_transient_db_error_operational():
    assert is_transient_db_error(OperationalError("stmt", {}, Exception("connection refused")))


def test_is_transient_db_error_non_transient():
    assert not is_transient_db_error(ValueError("invalid data"))


def test_with_retry_succeeds_first_attempt():
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        return 42

    assert with_retry(op, max_attempts=3, delay_seconds=0, backoff=2, operation_name="test") == 42
    assert calls["n"] == 1


def test_with_retry_retries_transient_error():
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise OperationalError("stmt", {}, Exception("timeout"))
        return "ok"

    with patch("ingestor.retry.time.sleep"):
        result = with_retry(op, max_attempts=3, delay_seconds=1, backoff=2, operation_name="test")

    assert result == "ok"
    assert calls["n"] == 3


def test_with_retry_raises_non_transient_immediately():
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        raise ValueError("bad value")

    with pytest.raises(ValueError, match="bad value"):
        with_retry(op, max_attempts=3, delay_seconds=0, backoff=2, operation_name="test")

    assert calls["n"] == 1


def test_with_retry_exhausts_attempts():
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        raise OperationalError("stmt", {}, Exception("down"))

    with patch("ingestor.retry.time.sleep"):
        with pytest.raises(OperationalError):
            with_retry(op, max_attempts=2, delay_seconds=0, backoff=2, operation_name="test")

    assert calls["n"] == 2
