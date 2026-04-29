"""
Moteur de règles anti-fraude PySpark — 5 règles métier.

Produit la table fraud_assessment avec fraud_score (0-100)
= somme pondérée des flags activés.
"""

from __future__ import annotations

import uuid
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

from src.utils.config import FRAUD_CONFIG, GOLD_PATH, SILVER_PATH
from src.utils.logger import pipeline_step
from src.utils.spark_session import get_spark_session

# ── Constantes ─────────────────────────────────────────────────────────────────

WEIGHTS = FRAUD_CONFIG.rule_weights


# ── RULE_001 : early_claim ─────────────────────────────────────────────────────

def rule_001_early_claim(
    claims_df: DataFrame,
    policies_df: DataFrame,
    threshold_hours: int = FRAUD_CONFIG.early_claim_hours,
) -> DataFrame:
    """RULE_001 : sinistre déclaré < N heures après souscription.

    Args:
        claims_df: DataFrame claims Silver.
        policies_df: DataFrame policies Silver (une ligne par policy_id).
        threshold_hours: Seuil en heures (défaut : 48).

    Returns:
        DataFrame claims avec colonne flag_early_claim (0/1).
    """
    threshold_days = threshold_hours / 24.0

    # date_souscription is already present in Silver enriched claims — skip join
    if "date_souscription" in claims_df.columns:
        df = claims_df
    else:
        pol = (
            policies_df
            .dropDuplicates(["policy_id"])
            .select("policy_id", "date_souscription")
        )
        df = claims_df.join(pol, on="policy_id", how="left")

    return (
        df
        .withColumn(
            "days_since_souscription",
            F.datediff(F.col("date_sinistre"), F.col("date_souscription"))
        )
        .withColumn(
            "flag_early_claim",
            F.when(
                (F.col("days_since_souscription") >= 0) &
                (F.col("days_since_souscription") < threshold_days),
                F.lit(1)
            ).otherwise(F.lit(0)).cast(IntegerType())
        )
        .drop("days_since_souscription")
    )


# ── RULE_002 : high_amount ─────────────────────────────────────────────────────

def rule_002_high_amount(
    claims_df: DataFrame,
    n_stddev: float = FRAUD_CONFIG.high_amount_stddev_multiplier,
) -> DataFrame:
    """RULE_002 : montant déclaré > mean + N × stddev par produit.

    Args:
        claims_df: DataFrame claims Silver (avec colonne produit).
        n_stddev: Multiplicateur d'écart-type (défaut : 3.0).

    Returns:
        DataFrame avec colonne flag_high_amount (0/1).
    """
    stats = (
        claims_df
        .groupBy("produit")
        .agg(
            F.mean("montant_declare").alias("mean_montant"),
            F.stddev("montant_declare").alias("std_montant"),
        )
    )

    return (
        claims_df
        .join(stats, on="produit", how="left")
        .withColumn(
            "flag_high_amount",
            F.when(
                F.col("montant_declare") > (
                    F.col("mean_montant") + n_stddev * F.coalesce(F.col("std_montant"), F.lit(0.0))
                ),
                F.lit(1)
            ).otherwise(F.lit(0)).cast(IntegerType())
        )
        .drop("mean_montant", "std_montant")
    )


# ── RULE_003 : frequent_claimant ───────────────────────────────────────────────

def rule_003_frequent_claimant(
    claims_df: DataFrame,
    count_threshold: int = FRAUD_CONFIG.frequent_claimant_count,
    window_days: int = FRAUD_CONFIG.frequent_claimant_window_days,
) -> DataFrame:
    """RULE_003 : client avec N+ sinistres sur une fenêtre glissante de K jours.

    Args:
        claims_df: DataFrame claims Silver.
        count_threshold: Nombre de sinistres déclenchant le flag (défaut : 3).
        window_days: Fenêtre temporelle en jours (défaut : 365).

    Returns:
        DataFrame avec colonne flag_frequent_claimant (0/1).
    """
    # Self-join pour compter les sinistres dans la fenêtre glissante
    left  = claims_df.select("claim_id", "client_id",
                             F.col("date_sinistre").alias("ds_left"))
    right = claims_df.select("client_id",
                             F.col("date_sinistre").alias("ds_right"))

    window_counts = (
        left
        .join(right, on="client_id", how="inner")
        .filter(
            (F.datediff(F.col("ds_left"), F.col("ds_right")) >= 0) &
            (F.datediff(F.col("ds_left"), F.col("ds_right")) <= window_days)
        )
        .groupBy("claim_id")
        .agg(F.count("ds_right").alias("claims_in_window"))
    )

    return (
        claims_df
        .join(window_counts, on="claim_id", how="left")
        .withColumn(
            "flag_frequent_claimant",
            F.when(
                F.coalesce(F.col("claims_in_window"), F.lit(0)) >= count_threshold,
                F.lit(1)
            ).otherwise(F.lit(0)).cast(IntegerType())
        )
        .drop("claims_in_window")
    )


# ── RULE_004 : address_cluster ─────────────────────────────────────────────────

def rule_004_address_cluster(
    claims_df: DataFrame,
    clients_df: DataFrame,
    window_days: int = FRAUD_CONFIG.address_cluster_days,
) -> DataFrame:
    """RULE_004 : même adresse, produits différents, sinistres à < K jours d'écart.

    Args:
        claims_df: DataFrame claims Silver (avec produit).
        clients_df: DataFrame clients Silver (avec adresse).
        window_days: Fenêtre en jours (défaut : 30).

    Returns:
        DataFrame avec colonne flag_address_cluster (0/1).
    """
    clients_addr = (
        clients_df
        .dropDuplicates(["client_id"])
        .select("client_id", "adresse")
    )

    enriched = claims_df.join(clients_addr, on="client_id", how="left")

    left  = enriched.select(
        "claim_id",
        F.col("client_id").alias("cid_l"),
        F.col("adresse").alias("addr_l"),
        F.col("produit").alias("prod_l"),
        F.col("date_sinistre").alias("ds_l"),
    )
    right = enriched.select(
        F.col("claim_id").alias("claim_id_r"),
        F.col("client_id").alias("cid_r"),
        F.col("adresse").alias("addr_r"),
        F.col("produit").alias("prod_r"),
        F.col("date_sinistre").alias("ds_r"),
    )

    cluster_ids = (
        left
        .join(right,
              (F.col("addr_l") == F.col("addr_r")) &
              (F.col("cid_l") != F.col("cid_r")) &
              (F.col("prod_l") != F.col("prod_r")) &
              (F.abs(F.datediff(F.col("ds_l"), F.col("ds_r"))) <= window_days),
              how="inner")
        .select("claim_id")
        .distinct()
        .withColumn("flag_address_cluster", F.lit(1).cast(IntegerType()))
    )

    return (
        claims_df
        .join(cluster_ids, on="claim_id", how="left")
        .withColumn(
            "flag_address_cluster",
            F.coalesce(F.col("flag_address_cluster"), F.lit(0)).cast(IntegerType())
        )
    )


# ── RULE_005 : late_declaration ────────────────────────────────────────────────

def rule_005_late_declaration(
    claims_df: DataFrame,
    threshold_days: int = FRAUD_CONFIG.late_declaration_days,
) -> DataFrame:
    """RULE_005 : délai de déclaration > N jours.

    Args:
        claims_df: DataFrame claims Silver avec delai_declaration_jours.
        threshold_days: Seuil en jours (défaut : 180).

    Returns:
        DataFrame avec colonne flag_late_declaration (0/1).
    """
    return claims_df.withColumn(
        "flag_late_declaration",
        F.when(
            F.coalesce(F.col("delai_declaration_jours"), F.lit(0)) > threshold_days,
            F.lit(1)
        ).otherwise(F.lit(0)).cast(IntegerType())
    )


# ── Score agrégé ───────────────────────────────────────────────────────────────

def compute_fraud_score(df: DataFrame) -> DataFrame:
    """Calcule le fraud_score global (0-100) = somme pondérée des flags.

    Les poids sont définis dans config.FRAUD_CONFIG.rule_weights.

    Args:
        df: DataFrame avec tous les flags (0/1).

    Returns:
        DataFrame avec colonne fraud_score et fraud_flags (liste).
    """
    score_expr = (
        F.col("flag_early_claim")        * WEIGHTS["RULE_001_early_claim"] +
        F.col("flag_high_amount")        * WEIGHTS["RULE_002_high_amount"] +
        F.col("flag_frequent_claimant")  * WEIGHTS["RULE_003_frequent_claimant"] +
        F.col("flag_address_cluster")    * WEIGHTS["RULE_004_address_cluster"] +
        F.col("flag_late_declaration")   * WEIGHTS["RULE_005_late_declaration"]
    )

    max_score = sum(WEIGHTS.values())  # 100

    fraud_flags_expr = F.array(
        F.when(F.col("flag_early_claim")       == 1, F.lit("RULE_001")).otherwise(F.lit(None)),
        F.when(F.col("flag_high_amount")        == 1, F.lit("RULE_002")).otherwise(F.lit(None)),
        F.when(F.col("flag_frequent_claimant")  == 1, F.lit("RULE_003")).otherwise(F.lit(None)),
        F.when(F.col("flag_address_cluster")    == 1, F.lit("RULE_004")).otherwise(F.lit(None)),
        F.when(F.col("flag_late_declaration")   == 1, F.lit("RULE_005")).otherwise(F.lit(None)),
    )

    return (
        df
        .withColumn("fraud_score",
                    F.least(F.lit(100), F.greatest(F.lit(0), score_expr)))
        .withColumn("fraud_flags",
                    F.array_compact(fraud_flags_expr))
        .withColumn("is_fraud_suspected",
                    F.col("fraud_score") > 0)
        .withColumn("fraud_assessment_at", F.current_timestamp())
    )


# ── Pipeline principal ─────────────────────────────────────────────────────────

def run_fraud_flags(
    spark: Optional[SparkSession] = None,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    batch_id: Optional[str] = None,
) -> dict[str, int]:
    """Exécute le moteur de règles anti-fraude et écrit la table fraud_assessment.

    Args:
        spark: SparkSession (créée si None).
        silver_path: Chemin Silver source.
        gold_path: Chemin Gold cible.
        batch_id: Identifiant batch.

    Returns:
        Dictionnaire avec statistiques fraude.
    """
    _spark = spark or get_spark_session()
    _batch_id = batch_id or str(uuid.uuid4())[:8]

    with pipeline_step("fraud_flags", batch_id=_batch_id) as log:

        log.info("Loading Silver tables")
        claims_df  = _spark.read.format("delta").load(f"{silver_path}/claims")
        policies_df = _spark.read.format("delta").load(f"{silver_path}/policies")
        clients_df  = _spark.read.format("delta").load(f"{silver_path}/clients")

        n_in = claims_df.count()
        log.info("Claims loaded", n_in=n_in)

        log.info("Applying RULE_001 early_claim")
        df = rule_001_early_claim(claims_df, policies_df)

        log.info("Applying RULE_002 high_amount")
        df = rule_002_high_amount(df)

        log.info("Applying RULE_003 frequent_claimant")
        df = rule_003_frequent_claimant(df)

        log.info("Applying RULE_004 address_cluster")
        df = rule_004_address_cluster(df, clients_df)

        log.info("Applying RULE_005 late_declaration")
        df = rule_005_late_declaration(df)

        log.info("Computing fraud scores")
        df = compute_fraud_score(df)

        # Table fraud_assessment : uniquement les colonnes pertinentes
        assessment_cols = [
            "claim_id", "policy_id", "client_id", "produit",
            "date_sinistre", "montant_declare",
            "flag_early_claim", "flag_high_amount", "flag_frequent_claimant",
            "flag_address_cluster", "flag_late_declaration",
            "fraud_score", "fraud_flags", "is_fraud_suspected",
            "fraud_assessment_at",
        ]

        fraud_df = df.select(*[c for c in assessment_cols if c in df.columns])

        n_out = fraud_df.count()
        n_suspected = fraud_df.filter(F.col("is_fraud_suspected")).count()

        fraud_df.write.format("delta").mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{gold_path}/fraud_assessment")

        log.complete(
            nb_records_out=n_out,
            n_suspected=n_suspected,
            fraud_rate_pct=round(n_suspected / n_out * 100, 2) if n_out > 0 else 0,
        )

    return {
        "n_in":        n_in,
        "n_out":       n_out,
        "n_suspected": n_suspected,
    }


if __name__ == "__main__":
    stats = run_fraud_flags()
    print(f"Fraud flags terminé : {stats}")
