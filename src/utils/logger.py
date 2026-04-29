"""
Logger structuré JSON pour le pipeline AXA Claims Lakehouse.

Fournit un contexte de pipeline enrichi (step, batch_id, nb_records_in/out)
compatible avec Azure Monitor / ELK / Datadog.
"""

from __future__ import annotations

import logging
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

import structlog

from src.utils.config import LOG_LEVEL, LOG_FORMAT

# ── Structlog configuration ────────────────────────────────────────────────────

def _add_timestamp(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def _add_app_context(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    event_dict.setdefault("app", "axa-claims-lakehouse")
    return event_dict


def configure_logging() -> None:
    """Configure structlog avec rendu JSON ou console selon l'environnement."""
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        _add_timestamp,
        _add_app_context,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if LOG_FORMAT == "json":
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))


configure_logging()


# ── Public API ─────────────────────────────────────────────────────────────────

def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Retourne un logger nommé structlog.

    Args:
        name: Nom du module/composant (ex. 'ingestion.claims').

    Returns:
        BoundLogger prêt à l'emploi.
    """
    return structlog.get_logger(name)


class PipelineLogger:
    """Logger enrichi avec contexte pipeline (step, batch_id, volumétrie).

    Exemple::

        with PipelineLogger("bronze_to_silver", batch_id="abc") as log:
            log.info("Lecture source", nb_records_in=120_000)
            ...
            log.complete(nb_records_out=119_500, nb_rejected=500)
    """

    def __init__(
        self,
        step: str,
        batch_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.step = step
        self.batch_id = batch_id or str(uuid.uuid4())[:8]
        self._extra = extra or {}
        self._logger = get_logger(step)
        self._start: datetime | None = None

    def __enter__(self) -> "PipelineLogger":
        self._start = datetime.now(timezone.utc)
        structlog.contextvars.bind_contextvars(
            step=self.step,
            batch_id=self.batch_id,
            **self._extra,
        )
        self._logger.info("Pipeline step started", step=self.step, batch_id=self.batch_id)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        duration = (datetime.now(timezone.utc) - self._start).total_seconds()  # type: ignore[operator]
        if exc_type:
            self._logger.error(
                "Pipeline step FAILED",
                step=self.step,
                duration_sec=round(duration, 2),
                error=str(exc_val),
            )
        else:
            self._logger.info(
                "Pipeline step completed",
                step=self.step,
                duration_sec=round(duration, 2),
            )
        structlog.contextvars.clear_contextvars()
        return False  # don't suppress exceptions

    def info(self, event: str, **kw: Any) -> None:
        """Log niveau INFO avec contexte pipeline."""
        self._logger.info(event, **kw)

    def warning(self, event: str, **kw: Any) -> None:
        """Log niveau WARNING."""
        self._logger.warning(event, **kw)

    def error(self, event: str, **kw: Any) -> None:
        """Log niveau ERROR."""
        self._logger.error(event, **kw)

    def complete(self, nb_records_out: int, nb_rejected: int = 0, **kw: Any) -> None:
        """Log de fin d'étape avec volumétrie."""
        self._logger.info(
            "Step volumetry",
            nb_records_out=nb_records_out,
            nb_rejected=nb_rejected,
            **kw,
        )


@contextmanager
def pipeline_step(
    step: str,
    batch_id: str | None = None,
    **extra: Any,
) -> Generator[PipelineLogger, None, None]:
    """Context manager pour encapsuler une étape pipeline.

    Args:
        step: Nom de l'étape.
        batch_id: Identifiant du batch (généré si absent).
        **extra: Paires clé-valeur supplémentaires à logger.

    Yields:
        PipelineLogger configuré.
    """
    pl = PipelineLogger(step, batch_id, extra)
    with pl:
        yield pl
