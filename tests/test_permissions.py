import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestor.config import AppSettings, parse_permission_mode
from ingestor.permissions import (
    chmod_path,
    configure_ssh_client,
    grant_db_permissions,
    mkdir_p,
)


def test_parse_permission_mode():
    assert parse_permission_mode("775", 0) == 0o775
    assert parse_permission_mode("0o664", 0) == 0o664
    assert parse_permission_mode("", 0o755) == 0o755


def test_mkdir_p_applies_permissions():
    settings = AppSettings(auto_grant_permissions=True, dir_permission_mode=0o775)
    with tempfile.TemporaryDirectory() as tmp:
        path = mkdir_p(settings, Path(tmp) / "nested" / "dir")
        assert path.is_dir()
        if os.name != "nt":
            mode = path.stat().st_mode & 0o777
            assert mode == 0o775


def test_chmod_path_skipped_when_disabled():
    settings = AppSettings(auto_grant_permissions=False)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "file.txt"
        path.write_text("x", encoding="utf-8")
        chmod_path(settings, path, is_dir=False)


def test_configure_ssh_client_auto_accept():
    settings = AppSettings(ssh_auto_accept_host_key=True)
    ssh = configure_ssh_client(settings)
    assert ssh is not None


def test_grant_db_permissions_executes_statements():
    settings = AppSettings(
        auto_grant_permissions=True,
        db=__import__("ingestor.config", fromlist=["DatabaseSettings"]).DatabaseSettings(
            host="localhost",
            port=5432,
            name="biometrics",
            user="biometrics_user",
            password="x",
            schema="public",
        ),
    )
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    grant_db_permissions(settings, engine)

    assert conn.execute.call_count == 5
    conn.commit.assert_called_once()


def test_grant_db_permissions_invalid_user():
    settings = AppSettings(
        auto_grant_permissions=True,
        db=__import__("ingestor.config", fromlist=["DatabaseSettings"]).DatabaseSettings(
            host="localhost",
            port=5432,
            name="biometrics",
            user="bad-user",
            password="x",
        ),
    )
    with pytest.raises(ValueError):
        grant_db_permissions(settings, MagicMock())
