"""
Business view : Loss Ratio.

loss_ratio = incurred_losses / earned_premium × 100
Par produit / région / canal / trimestre + YTD + rolling 12 mois.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

from src.utils.config import GOLD_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("business_views.loss_ratio")


def compute_loss_ratio(
    spark: Optional[SparkSession] = None,
    gold_path: str = GOLD_PATH,
    output_path: Optional[str] = None,
) -> DataFrame:
    """Calcule et persiste le loss ratio multi-dimensionnel.

    Dimensions : produit × region_code × canal_vente × trimestre.
    Métriques : loss_ratio trimestriel, YTD, rolling 12 mois.

    Args:
        spark: SparkSession active.
        gold_path: Chemin Gold.
        output_path: Chemin de sortie (défaut : gold_path/loss_ratio).

    Returns:
        DataFrame loss ratio Gold.
    """
    _spark = spark or get_spark_session()
    _out = output_path or f"{gold_path}/loss_ratio"

    log.info("Loading portfolio_kpis Gold table")
    kpis = _spark.read.format("delta").load(f"{gold_path}/portfolio_kpis")

    # ── Trimestriel ─────────────────────────────────────────────────────────────
    quarterly = (
        kpis
        .withColumn(
            "loss_ratio_pct",
            F.when(F.col("earned_premium") > 0,
                   F.round(F.col("incurred_losses") / F.col("earned_premium") * 100, 4))
            .otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # ── YTD (Year-to-Date) ──────────────────────────────────────────────────────
    w_ytd = (
        Window
        .partitionBy("produit", "region_code", "canal_vente", "annee")
        .orderBy("trimestre")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    quarterly = (
        quarterly
        .withColumn("ytd_earned_premium",  F.sum("earned_premium").over(w_ytd))
        .withColumn("ytd_incurred_losses", F.sum("incurred_losses").over(w_ytd))
        .withColumn(
            "loss_ratio_ytd_pct",
            F.when(F.col("ytd_earned_premium") > 0,
                   F.round(F.col("ytd_incurred_losses") / F.col("ytd_earned_premium") * 100, 4))
            .otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # ── Rolling 12 mois (4 trimestres) ─────────────────────────────────────────
    w_roll12 = (
        Window
        .partitionBy("produit", "region_code", "canal_vente")
        .orderBy("trimestre")
        .rowsBetween(-3, Window.currentRow)
    )

    quarterly = (
        quarterly
        .withColumn("rolling_12m_premium",  F.sum("earned_premium").over(w_roll12))
        .withColumn("rolling_12m_losses",   F.sum("incurred_losses").over(w_roll12))
        .withColumn(
            "loss_ratio_rolling_12m_pct",
            F.when(F.col("rolling_12m_premium") > 0,
                   F.round(F.col("rolling_12m_losses") / F.col("rolling_12m_premium") * 100, 4))
            .otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # ── Ajout classification ────────────────────────────────────────────────────
    quarterly = quarterly.withColumn(
        "loss_ratio_category",
        F.when(F.col("loss_ratio_pct") < 60, F.lit("excellent"))
        .when(F.col("loss_ratio_pct") < 80, F.lit("bon"))
        .when(F.col("loss_ratio_pct") < 100, F.lit("acceptable"))
        .when(F.col("loss_ratio_pct") < 150, F.lit("dégradé"))
        .otherwise(F.lit("critique"))
    )

    # ── Écriture ────────────────────────────────────────────────────────────────
    (
        quarterly.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("annee", "produit")
        .save(_out)
    )

    n = quarterly.count()
    log.info("Loss ratio table written", n_rows=n, path=_out)
    return quarterly


if __name__ == "__main__":
    df = compute_loss_ratio()
    df.show(10, truncate=False)
