import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, TypeVar

from .config import AppSettings, project_root, resolve_root_dir
from .permissions import mkdir_p

logger = logging.getLogger(__name__)

T = TypeVar("T")

# errno: ENOSPC (Unix), ERROR_DISK_FULL (Windows)
_DISK_FULL_ERRNOS = {28, 112}


@dataclass
class PurgeResult:
    files_deleted: int
    bytes_freed: int
    free_mb_before: float
    free_mb_after: float


def _project_root() -> Path:
    return project_root()


def resolve_data_path(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_absolute():
        path = project_root() / path_str
    return path


def get_disk_reference_path(settings: AppSettings) -> Path:
    """Chemin de référence pour mesurer l'espace disque."""
    return resolve_root_dir(settings.root_dir)


def get_free_space_mb(path: Path) -> float:
    usage = shutil.disk_usage(str(path))
    return usage.free / (1024 * 1024)


def is_disk_space_low(settings: AppSettings) -> bool:
    if not settings.disk_purge_enabled:
        return False
    ref = mkdir_p(settings, get_disk_reference_path(settings))
    return get_free_space_mb(ref) < settings.disk_min_free_mb


def is_disk_full_error(exc: BaseException) -> bool:
    if isinstance(exc, OSError):
        if exc.errno in _DISK_FULL_ERRNOS:
            return True
        msg = str(exc).lower()
        if "no space left" in msg or "disk full" in msg or "not enough space" in msg:
            return True
    return False


def _purgeable_dirs(settings: AppSettings) -> List[Path]:
    """Répertoires purgeables, du moins au plus critique (archives et logs)."""
    return [
        resolve_data_path(settings.execution_log_dir),
        resolve_data_path(settings.archive_json_dir),
        resolve_data_path(settings.archive_dir),
        resolve_data_path(settings.error_storage_dir),
    ]


def _iter_purgeable_files(directory: Path) -> List[Path]:
    if not directory.exists():
        return []
    return [p for p in directory.rglob("*") if p.is_file()]


def purge_old_files(settings: AppSettings) -> PurgeResult:
    """
    Supprime les fichiers les plus anciens dans les répertoires d'archives et de logs
    jusqu'à atteindre DISK_PURGE_TARGET_FREE_MB ou DISK_PURGE_MAX_FILES.
    """
    ref = mkdir_p(settings, get_disk_reference_path(settings))

    free_before = get_free_space_mb(ref)
    files_deleted = 0
    bytes_freed = 0
    target_free = settings.disk_purge_target_free_mb
    max_files = settings.disk_purge_max_files

    for purge_dir in _purgeable_dirs(settings):
        if get_free_space_mb(ref) >= target_free or files_deleted >= max_files:
            break

        candidates = _iter_purgeable_files(purge_dir)
        candidates.sort(key=lambda p: p.stat().st_mtime)

        for file_path in candidates:
            if get_free_space_mb(ref) >= target_free or files_deleted >= max_files:
                break
            try:
                size = file_path.stat().st_size
                file_path.unlink()
                files_deleted += 1
                bytes_freed += size
                logger.info("Purge: supprimé %s (%.1f Ko)", file_path, size / 1024)
            except OSError as exc:
                logger.warning("Purge: impossible de supprimer %s: %s", file_path, exc)

    free_after = get_free_space_mb(ref)
    return PurgeResult(
        files_deleted=files_deleted,
        bytes_freed=bytes_freed,
        free_mb_before=free_before,
        free_mb_after=free_after,
    )


def purge_if_needed(settings: AppSettings) -> Optional[PurgeResult]:
    """Lance une purge si l'espace libre est sous le seuil configuré."""
    if not settings.disk_purge_enabled:
        return None

    ref = get_disk_reference_path(settings)
    free_mb = get_free_space_mb(ref)

    if free_mb >= settings.disk_min_free_mb:
        return None

    logger.warning(
        "Espace disque faible: %.1f Mo libres (seuil: %d Mo). Purge automatique...",
        free_mb,
        settings.disk_min_free_mb,
    )
    result = purge_old_files(settings)
    logger.warning(
        "Purge terminée: %d fichier(s) supprimé(s), %.1f Mo libérés (%.1f -> %.1f Mo libres)",
        result.files_deleted,
        result.bytes_freed / (1024 * 1024),
        result.free_mb_before,
        result.free_mb_after,
    )
    return result


def ensure_disk_space(settings: AppSettings) -> bool:
    """
    Vérifie l'espace disque et purge si nécessaire.
    Retourne True si l'espace est suffisant après purge (ou déjà suffisant).
    """
    if not is_disk_space_low(settings):
        return True
    result = purge_if_needed(settings)
    if result is None:
        return True
    return get_free_space_mb(get_disk_reference_path(settings)) >= settings.disk_min_free_mb


def with_disk_purge_retry(settings: AppSettings, operation: Callable[[], T]) -> T:
    """Exécute une opération d'écriture ; purge et réessaie une fois si disque plein."""
    try:
        return operation()
    except OSError as exc:
        if not is_disk_full_error(exc):
            raise
        logger.warning("Disque plein détecté, tentative de purge automatique...")
        if ensure_disk_space(settings):
            return operation()
        raise
