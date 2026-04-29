"""
Business view : Claims Aging.

Distribution des délais de traitement en buckets temporels
(0-7j, 7-30j, 30-90j, 90j+) par type de sinistre.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config import GOLD_PATH, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("business_views.claims_aging")

# Bornes des buckets en jours
AGING_BUCKETS = [
    ("0_7j",   0,   7),
    ("7_30j",  7,   30),
    ("30_90j", 30,  90),
    ("90j_plus", 90, None),
]


def _bucket_label(days_col: str) -> "Column":  # type: ignore[name-defined]
    """Génère l'expression CASE WHEN pour le bucket aging.

    Args:
        days_col: Nom de la colonne délai en jours.

    Returns:
        Expression Column PySpark.
    """
    return (
        F.when(F.col(days_col) <= 7,  F.lit("0_7j"))
        .when(F.col(days_col) <= 30,  F.lit("7_30j"))
        .when(F.col(days_col) <= 90,  F.lit("30_90j"))
        .otherwise(F.lit("90j_plus"))
    )


def compute_processing_delay(df: DataFrame) -> DataFrame:
    """Calcule le délai de traitement = date_cloture - date_declaration.

    Les sinistres encore ouverts utilisent la date courante comme proxy.

    Args:
        df: DataFrame claims Silver.

    Returns:
        DataFrame avec colonne delai_traitement_jours.
    """
    reference_date = F.coalesce(F.col("date_cloture"), F.current_date())
    return df.withColumn(
        "delai_traitement_jours",
        F.greatest(
            F.datediff(reference_date, F.col("date_declaration")),
            F.lit(0)
        )
    )


def compute_claims_aging(
    spark: Optional[SparkSession] = None,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    output_path: Optional[str] = None,
) -> DataFrame:
    """Calcule la distribution des délais de traitement par type de sinistre.

    Produit deux tables :
    - claims_aging_detail : ligne par sinistre avec bucket
    - claims_aging_summary : agrégation par type × bucket × statut

    Args:
        spark: SparkSession active.
        silver_path: Chemin Silver.
        gold_path: Chemin Gold.
        output_path: Chemin Gold de sortie.

    Returns:
        DataFrame résumé aging.
    """
    _spark = spark or get_spark_session()
    _out = output_path or f"{gold_path}/claims_aging"

    log.info("Loading claims Silver")
    claims = _spark.read.format("delta").load(f"{silver_path}/claims")

    # Calcul délai traitement
    claims = compute_processing_delay(claims)

    # Bucket aging
    detail = (
        claims
        .withColumn("aging_bucket", _bucket_label("delai_traitement_jours"))
        .withColumn("annee",        F.col("annee_sinistre"))
    )

    # Résumé par type_sinistre × bucket × statut × annee
    summary = (
        detail
        .groupBy("type_sinistre", "aging_bucket", "statut", "annee", "produit")
        .agg(
            F.count("claim_id").alias("nb_sinistres"),
            F.sum("montant_declare").alias("montant_declare_total"),
            F.sum("montant_indemnise").alias("montant_indemnise_total"),
            F.avg("delai_traitement_jours").alias("delai_moyen_jours"),
            F.percentile_approx("delai_traitement_jours", 0.5).alias("delai_median_jours"),
            F.percentile_approx("delai_traitement_jours", 0.9).alias("delai_p90_jours"),
        )
        .withColumn("aging_computed_at", F.current_timestamp())
    )

    # Ordre des buckets pour affichage
    bucket_order = F.when(F.col("aging_bucket") == "0_7j",    F.lit(1)) \
                    .when(F.col("aging_bucket") == "7_30j",   F.lit(2)) \
                    .when(F.col("aging_bucket") == "30_90j",  F.lit(3)) \
                    .otherwise(F.lit(4))

    summary = summary.withColumn("bucket_order", bucket_order)

    # Écriture detail
    (
        detail.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("annee", "type_sinistre")
        .save(f"{_out}_detail")
    )

    # Écriture summary
    (
        summary.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(f"{_out}_summary")
    )

    n = summary.count()
    log.info("Claims aging written", n_rows_summary=n)
    return summary


if __name__ == "__main__":
    df = compute_claims_aging()
    df.orderBy("type_sinistre", "bucket_order").show(40, truncate=False)
