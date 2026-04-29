"""
Métriques d'exécution du pipeline AXA Claims Lakehouse.

Collecte : temps par step, volumétrie in/out, taux de rejet,
records frauduleux détectés. Persiste en JSON + Delta Lake.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from src.utils.config import GOLD_PATH, REPORTS_DIR, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("monitoring.pipeline_metrics")


# ── Collecteur de métriques en mémoire ────────────────────────────────────────

class MetricsCollector:
    """Collecteur de métriques pipeline thread-safe.

    Accumule les métriques par step et les sérialise en JSON.
    """

    _instance: "MetricsCollector | None" = None

    def __init__(self) -> None:
        self._metrics: list[dict[str, Any]] = []
        self._run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    @classmethod
    def get_instance(cls) -> "MetricsCollector":
        """Singleton."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def record(self, step: str, **kw: Any) -> None:
        """Enregistre une métrique de step.

        Args:
            step: Nom de l'étape.
            **kw: Métriques clé-valeur (duration_sec, nb_in, nb_out, ...).
        """
        self._metrics.append({
            "run_id":    self._run_id,
            "step":      step,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kw,
        })
        log.info("Metric recorded", step=step, **kw)

    def to_dict(self) -> dict[str, Any]:
        """Sérialise toutes les métriques collectées."""
        return {
            "run_id":  self._run_id,
            "steps":   self._metrics,
            "summary": self._compute_summary(),
        }

    def _compute_summary(self) -> dict[str, Any]:
        """Calcule un résumé agrégé des métriques."""
        total_duration = sum(m.get("duration_sec", 0) for m in self._metrics)
        total_rejected = sum(m.get("nb_rejected", 0) for m in self._metrics)
        total_out = sum(m.get("nb_out", 0) for m in self._metrics)
        return {
            "total_duration_sec": round(total_duration, 2),
            "total_records_out":  total_out,
            "total_rejected":     total_rejected,
            "rejection_rate_pct": round(total_rejected / max(total_out, 1) * 100, 4),
        }

    def save_json(self, path: Path | None = None) -> Path:
        """Sauvegarde les métriques en JSON.

        Args:
            path: Chemin de sortie (défaut : reports/pipeline_metrics_{run_id}.json).

        Returns:
            Path vers le fichier JSON.
        """
        _path = path or (REPORTS_DIR / f"pipeline_metrics_{self._run_id}.json")
        _path.write_text(json.dumps(self.to_dict(), indent=2, default=str), encoding="utf-8")
        log.info("Metrics saved", path=str(_path))
        return _path


@contextmanager
def timed_step(
    step_name: str,
    collector: MetricsCollector | None = None,
    **extra: Any,
) -> Generator[dict[str, Any], None, None]:
    """Context manager qui mesure la durée d'une étape et l'enregistre.

    Args:
        step_name: Nom de l'étape.
        collector: Collecteur de métriques (singleton si None).
        **extra: Métriques initiales (ex. nb_in=1000).

    Yields:
        Dictionnaire de métriques modifiable pendant l'étape.
    """
    _collector = collector or MetricsCollector.get_instance()
    metrics: dict[str, Any] = {"nb_in": 0, "nb_out": 0, "nb_rejected": 0, **extra}
    start = time.perf_counter()

    try:
        yield metrics
    finally:
        duration = time.perf_counter() - start
        metrics["duration_sec"] = round(duration, 3)
        _collector.record(step_name, **metrics)


# ── Collecte post-pipeline ─────────────────────────────────────────────────────

def collect_metrics(
    run_date: str | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Collecte les métriques d'exécution depuis les tables Gold.

    Args:
        run_date: Date d'exécution (format YYYY-MM-DD).
        output_dir: Répertoire de sortie pour le rapport JSON.

    Returns:
        Dictionnaire de métriques collectées.
    """
    _run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _output_dir = output_dir or REPORTS_DIR

    log.info("Collecting pipeline metrics", run_date=_run_date)

    spark = get_spark_session(app_name="pipeline_metrics")
    metrics: dict[str, Any] = {
        "run_date":    _run_date,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "tables":      {},
    }

    table_checks = [
        (SILVER_PATH, "claims",   "silver_claims"),
        (SILVER_PATH, "clients",  "silver_clients"),
        (SILVER_PATH, "policies", "silver_policies"),
        (GOLD_PATH,   "portfolio_kpis", "gold_portfolio_kpis"),
        (GOLD_PATH,   "fraud_assessment", "gold_fraud_assessment"),
        (GOLD_PATH,   "churn_scores",     "gold_churn_scores"),
        (GOLD_PATH,   "loss_ratio",       "gold_loss_ratio"),
    ]

    from pyspark.sql import functions as F

    for base_path, table, label in table_checks:
        try:
            df = spark.read.format("delta").load(f"{base_path}/{table}")
            n = df.count()

            table_metrics: dict[str, Any] = {"row_count": n}

            # Métriques spécifiques
            if table == "fraud_assessment":
                n_suspected = df.filter(F.col("is_fraud_suspected")).count()
                table_metrics["fraud_suspected_count"] = n_suspected
                table_metrics["fraud_rate_pct"] = round(n_suspected / max(n, 1) * 100, 4)

            elif table == "churn_scores":
                by_seg = {
                    row["churn_segment"]: row["count"]
                    for row in df.groupBy("churn_segment").count().collect()
                }
                table_metrics["churn_by_segment"] = by_seg

            elif table == "claims":
                n_fraud = df.filter(F.col("is_fraud_injected") == True).count()  # noqa: E712
                table_metrics["injected_fraud_count"] = n_fraud

            metrics["tables"][label] = table_metrics
            log.info(f"Metrics collected for {label}", **table_metrics)

        except Exception as e:
            log.warning(f"Cannot read {table}", error=str(e))
            metrics["tables"][label] = {"error": str(e)}

    # Sauvegarde JSON
    output_path = _output_dir / f"pipeline_metrics_{_run_date}.json"
    output_path.write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    log.info("Pipeline metrics saved", path=str(output_path))

    return metrics


# ── Utilitaires de monitoring en continu ──────────────────────────────────────

def get_latest_run_summary() -> dict[str, Any]:
    """Retourne le résumé du dernier run de pipeline depuis les fichiers JSON.

    Returns:
        Dictionnaire résumé ou dict vide si aucun run trouvé.
    """
    reports = sorted(REPORTS_DIR.glob("pipeline_metrics_*.json"))
    if not reports:
        return {}
    latest = reports[-1]
    return json.loads(latest.read_text(encoding="utf-8"))


if __name__ == "__main__":
    result = collect_metrics()
    print(json.dumps(result, indent=2, default=str))
