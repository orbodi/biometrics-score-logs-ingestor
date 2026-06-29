import tempfile
from pathlib import Path

from ingestor.config import AppSettings
from ingestor.state import (
    count_unresolved_errors,
    record_operation_error,
    resolve_operation_error,
    store_failed_resource,
)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        state_db_path=str(tmp_path / "state.db"),
        error_storage_dir=str(tmp_path / "errors"),
    )


def test_record_and_resolve_operation_error():
    with tempfile.TemporaryDirectory() as tmp:
        settings = _settings(Path(tmp))

        record_operation_error(
            settings,
            "db_persist",
            "/outputs/server1/file.jsonl",
            RuntimeError("connection refused"),
            server_name="server1",
            source_file="file.log",
        )
        assert count_unresolved_errors(settings) == 1
        assert count_unresolved_errors(settings, "db_persist") == 1

        record_operation_error(
            settings,
            "db_persist",
            "/outputs/server1/file.jsonl",
            RuntimeError("still down"),
        )
        assert count_unresolved_errors(settings) == 1

        resolve_operation_error(settings, "db_persist", "/outputs/server1/file.jsonl")
        assert count_unresolved_errors(settings) == 0


def test_store_failed_resource():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        settings = _settings(tmp_path)
        source = tmp_path / "sample.jsonl"
        source.write_text('{"rq_type": "IP"}\n', encoding="utf-8")

        dest = store_failed_resource(settings, source, "db_persist")
        assert dest.exists()
        assert dest.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")
