import tempfile
from pathlib import Path

from ingestor.config import AppSettings, workspace_paths_from_root
from ingestor.state import (
    is_file_already_downloaded,
    mark_file_downloaded,
    should_skip_file_copy,
)


def _settings(tmp_path: Path) -> AppSettings:
    paths = workspace_paths_from_root(str(tmp_path))
    return AppSettings(**paths)


def test_mark_file_downloaded_prevents_redownload():
    with tempfile.TemporaryDirectory() as tmp:
        settings = _settings(Path(tmp))
        mark_file_downloaded(settings, "mbss1", "quality.2026-06-29.log", file_size=1234)

        assert is_file_already_downloaded(settings, "mbss1", "quality.2026-06-29.log")
        skip, reason = should_skip_file_copy(settings, "mbss1", "quality.2026-06-29.log")
        assert skip is True
        assert reason == "déjà copié (state)"


def test_should_skip_when_file_in_archive():
    with tempfile.TemporaryDirectory() as tmp:
        settings = _settings(Path(tmp))
        archive = Path(settings.archive_dir) / "mbss1"
        archive.mkdir(parents=True)
        log_file = archive / "quality.2026-06-29.log"
        log_file.write_text("RqType=IP\n", encoding="utf-8")

        skip, reason = should_skip_file_copy(settings, "mbss1", "quality.2026-06-29.log")
        assert skip is True
        assert "archive" in reason
        assert is_file_already_downloaded(settings, "mbss1", "quality.2026-06-29.log")


def test_mark_file_downloaded_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        settings = _settings(Path(tmp))
        mark_file_downloaded(settings, "mbss1", "quality.log", file_size=100)
        mark_file_downloaded(settings, "mbss1", "quality.log", file_size=999)

        skip, _ = should_skip_file_copy(settings, "mbss1", "quality.log")
        assert skip is True
