"""
Business view : Churn Score client.

Score 0-100 basé sur :
  ancienneté_inverse × 0.3 + nb_impayés × 0.3
  + nb_sinistres_refusés × 0.2 + mono_contrat × 0.2
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.utils.config import BRONZE_PATH, CHURN_CONFIG, GOLD_PATH, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("business_views.churn_score")

W = CHURN_CONFIG


def _compute_seniority_score(clients_df: DataFrame) -> DataFrame:
    """Score d'ancienneté inversée : client récent = score élevé (risque churn fort).

    Normalise l'ancienneté sur [0, max_seniority_years] et inverse.

    Args:
        clients_df: DataFrame clients Silver.

    Returns:
        DataFrame avec score_seniority_inverse normalisé [0, 1].
    """
    max_years = W.max_seniority_years

    return (
        clients_df
        .withColumn(
            "anciennete_annees",
            F.datediff(F.current_date(), F.to_date(F.col("date_creation_client"))) / F.lit(365.0)
        )
        .withColumn(
            "anciennete_clipped",
            F.least(F.col("anciennete_annees"), F.lit(float(max_years)))
        )
        .withColumn(
            "score_seniority_inverse",
            F.lit(1.0) - F.col("anciennete_clipped") / F.lit(float(max_years))
        )
    )


def _compute_unpaid_score(clients_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    """Score impayés : ratio impayés / total paiements normalisé.

    Args:
        clients_df: DataFrame clients Silver.
        payments_df: DataFrame payments Silver.

    Returns:
        DataFrame avec score_unpaid [0, 1].
    """
    unpaid = (
        payments_df
        .groupBy("client_id")
        .agg(
            F.count("payment_id").alias("total_paiements"),
            F.sum(F.when(F.col("statut") == "impaye", 1).otherwise(0)).alias("nb_impayes"),
        )
        .withColumn(
            "score_unpaid",
            F.least(F.lit(1.0),
                    F.col("nb_impayes").cast(DoubleType()) / F.greatest(F.col("total_paiements").cast(DoubleType()), F.lit(1.0)))
        )
    )

    return clients_df.join(
        unpaid.select("client_id", "score_unpaid", "nb_impayes"),
        on="client_id", how="left"
    ).fillna(0.0, subset=["score_unpaid"])


def _compute_refused_claims_score(clients_df: DataFrame, claims_df: DataFrame) -> DataFrame:
    """Score sinistres refusés : ratio refusés/total normalisé.

    Args:
        clients_df: DataFrame clients Silver.
        claims_df: DataFrame claims Silver.

    Returns:
        DataFrame avec score_refused [0, 1].
    """
    refused = (
        claims_df
        .groupBy("client_id")
        .agg(
            F.count("claim_id").alias("total_sinistres"),
            F.sum(F.when(F.col("statut") == "refuse", 1).otherwise(0)).alias("nb_refuses"),
        )
        .withColumn(
            "score_refused",
            F.least(F.lit(1.0),
                    F.col("nb_refuses").cast(DoubleType()) / F.greatest(F.col("total_sinistres").cast(DoubleType()), F.lit(1.0)))
        )
    )

    return clients_df.join(
        refused.select("client_id", "score_refused", "nb_refuses"),
        on="client_id", how="left"
    ).fillna(0.0, subset=["score_refused"])


def _compute_mono_contract_score(clients_df: DataFrame, policies_df: DataFrame) -> DataFrame:
    """Score mono-contrat : client avec une seule police = risque churn plus élevé.

    Args:
        clients_df: DataFrame clients Silver.
        policies_df: DataFrame policies Silver.

    Returns:
        DataFrame avec score_mono_contract (0 si multi, 1 si mono).
    """
    contract_count = (
        policies_df
        .filter(F.col("statut") == "actif")
        .dropDuplicates(["policy_id"])
        .groupBy("client_id")
        .agg(F.countDistinct("policy_id").alias("nb_contrats"))
    )

    return (
        clients_df
        .join(contract_count.select("client_id", "nb_contrats"), on="client_id", how="left")
        .fillna(0, subset=["nb_contrats"])
        .withColumn(
            "score_mono_contract",
            F.when(F.col("nb_contrats") <= 1, F.lit(1.0)).otherwise(F.lit(0.0))
        )
    )


def compute_churn_scores(
    spark: Optional[SparkSession] = None,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    output_path: Optional[str] = None,
) -> DataFrame:
    """Calcule le score de churn pour chaque client et persiste la table Gold.

    Args:
        spark: SparkSession active.
        silver_path: Chemin Silver.
        gold_path: Chemin Gold.
        output_path: Chemin de sortie Gold (défaut : gold_path/churn_scores).

    Returns:
        DataFrame Gold churn_scores.
    """
    _spark = spark or get_spark_session()
    _out = output_path or f"{gold_path}/churn_scores"

    log.info("Loading Silver tables for churn")
    clients_df  = _spark.read.format("delta").load(f"{silver_path}/clients")
    policies_df = _spark.read.format("delta").load(f"{silver_path}/policies")
    claims_df   = _spark.read.format("delta").load(f"{silver_path}/claims")

    # On a besoin d'une seule ligne par client
    clients = clients_df.dropDuplicates(["client_id"])
    # Renommer pour éviter conflits de jointure
    clients = clients.withColumnRenamed("date_creation", "date_creation_client")

    log.info("Computing seniority scores")
    clients = _compute_seniority_score(clients)

    log.info("Computing unpaid scores")
    payments_df = _spark.read.format("delta").load(f"{BRONZE_PATH}/payments")
    clients = _compute_unpaid_score(clients, payments_df)

    log.info("Computing refused claims scores")
    clients = _compute_refused_claims_score(clients, claims_df)

    log.info("Computing mono-contract scores")
    clients = _compute_mono_contract_score(clients, policies_df)

    # ── Score final ─────────────────────────────────────────────────────────────
    churn_df = (
        clients
        .withColumn(
            "churn_score_raw",
            F.col("score_seniority_inverse") * F.lit(W.weight_seniority_inverse) +
            F.col("score_unpaid")            * F.lit(W.weight_unpaid) +
            F.col("score_refused")           * F.lit(W.weight_refused_claims) +
            F.col("score_mono_contract")     * F.lit(W.weight_mono_contract)
        )
        .withColumn(
            "churn_score",
            F.round(
                F.least(F.lit(100.0), F.greatest(F.lit(0.0),
                        F.col("churn_score_raw") * F.lit(100.0))),
                2
            )
        )
        .withColumn(
            "churn_segment",
            F.when(F.col("churn_score") < 25, F.lit("faible"))
            .when(F.col("churn_score") < 50, F.lit("moyen"))
            .when(F.col("churn_score") < 75, F.lit("élevé"))
            .otherwise(F.lit("critique"))
        )
        .withColumn("churn_computed_at", F.current_timestamp())
    )

    output_cols = [
        "client_id", "client_type", "nom", "region_code", "segment",
        "anciennete_annees", "nb_contrats", "nb_impayes", "nb_refuses",
        "score_seniority_inverse", "score_unpaid", "score_refused", "score_mono_contract",
        "churn_score", "churn_segment", "churn_computed_at",
    ]
    final_df = churn_df.select(*[c for c in output_cols if c in churn_df.columns])

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(_out)
    )

    n = final_df.count()
    by_segment = {
        row["churn_segment"]: row["count"]
        for row in final_df.groupBy("churn_segment").count().collect()
    }
    log.info("Churn scores written", n_clients=n, by_segment=by_segment)
    return final_df


if __name__ == "__main__":
    df = compute_churn_scores()
    df.orderBy(F.col("churn_score").desc()).show(20)
