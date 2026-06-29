import errno
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from ingestor.config import AppSettings, workspace_paths_from_root
from ingestor.disk_purge import (
    is_disk_full_error,
    purge_if_needed,
    purge_old_files,
    with_disk_purge_retry,
)


def test_is_disk_full_error_enospc():
    assert is_disk_full_error(OSError(errno.ENOSPC, "No space left on device"))


def test_is_disk_full_error_other():
    assert not is_disk_full_error(ValueError("bad"))


def test_purge_deletes_oldest_files_first():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()

        old_file = logs_dir / "old.log"
        new_file = logs_dir / "new.log"
        old_file.write_text("old", encoding="utf-8")
        new_file.write_text("new", encoding="utf-8")

        old_ts = time.time() - 3600
        os.utime(old_file, (old_ts, old_ts))

        settings = AppSettings(
            **workspace_paths_from_root(str(tmp_path)),
            disk_purge_target_free_mb=999999,
            disk_purge_max_files=1,
        )

        with patch("ingestor.disk_purge.get_free_space_mb", return_value=100.0):
            result = purge_old_files(settings)

        assert result.files_deleted == 1
        assert not old_file.exists()
        assert new_file.exists()


def test_purge_if_needed_skips_when_enough_space():
    settings = AppSettings(disk_min_free_mb=100)
    with patch("ingestor.disk_purge.get_free_space_mb", return_value=5000.0):
        assert purge_if_needed(settings) is None


def test_with_disk_purge_retry_on_disk_full():
    calls = {"n": 0}
    settings = AppSettings()

    def op():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(errno.ENOSPC, "No space left on device")
        return "ok"

    with patch("ingestor.disk_purge.ensure_disk_space", return_value=True):
        assert with_disk_purge_retry(settings, op) == "ok"
    assert calls["n"] == 2


def test_with_disk_purge_retry_raises_non_disk_error():
    settings = AppSettings()
    with pytest.raises(PermissionError):
        with_disk_purge_retry(settings, lambda: (_ for _ in ()).throw(PermissionError("denied")))
