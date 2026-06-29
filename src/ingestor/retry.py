import logging
import time
from typing import Callable, Optional, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def is_transient_db_error(exc: BaseException) -> bool:
    """Indique si l'erreur DB est probablement transitoire (connexion, timeout)."""
    if isinstance(exc, OperationalError):
        return True
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    cause = exc.__cause__
    if cause is not None and cause is not exc:
        return is_transient_db_error(cause)
    return False


def with_retry(
    operation: Callable[[], T],
    *,
    max_attempts: int,
    delay_seconds: float,
    backoff: float,
    operation_name: str = "operation",
) -> T:
    """
    Exécute une opération avec retry et backoff exponentiel sur erreurs transitoires DB.

    Les erreurs non transitoires sont propagées immédiatement.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempt = 0
    delay = delay_seconds
    last_exc: Optional[BaseException] = None

    while attempt < max_attempts:
        attempt += 1
        try:
            return operation()
        except Exception as exc:
            last_exc = exc
            if not is_transient_db_error(exc) or attempt >= max_attempts:
                raise

            logger.warning(
                "%s échouée (tentative %d/%d, retry dans %.1fs): %s",
                operation_name,
                attempt,
                max_attempts,
                delay,
                exc,
            )
            time.sleep(delay)
            delay *= backoff

    assert last_exc is not None
    raise last_exc
