import tempfile
from pathlib import Path

from ingestor.config import ensure_workspace_dirs, load_settings, workspace_paths_from_root


def test_workspace_paths_from_root():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "ingestor"
        paths = workspace_paths_from_root(str(root))
        assert Path(paths["input_dir"]) == root / "inputs"
        assert Path(paths["state_db_path"]) == root / "state" / "ingestor_state.db"


def test_ensure_workspace_dirs_creates_subdirs():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "workspace"
        paths = workspace_paths_from_root(str(root))
        from ingestor.config import AppSettings

        settings = AppSettings(**paths)
        ensure_workspace_dirs(settings)

        assert (root / "inputs").is_dir()
        assert (root / "outputs").is_dir()
        assert (root / "archive").is_dir()
        assert (root / "archive_json").is_dir()
        assert (root / "logs").is_dir()
        assert (root / "errors").is_dir()
        assert (root / "state").is_dir()


def test_load_settings_creates_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("ROOT_DIR", str(tmp_path / "data"))
    settings = load_settings()
    assert Path(settings.input_dir).is_dir()
    assert settings.input_dir == str((tmp_path / "data" / "inputs").resolve())
