"""
Transformation Silver → Gold.

Agrégation par produit × région × canal × trimestre,
calcul earned_premium et incurred_losses, écriture tables Gold analytiques.
"""

from __future__ import annotations

import uuid
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.utils.config import GOLD_PATH, SILVER_PATH
from src.utils.logger import pipeline_step
from src.utils.spark_session import get_spark_session


# ── Lecture Silver ─────────────────────────────────────────────────────────────

def read_silver(
    spark: SparkSession,
    entity: str,
    silver_path: str = SILVER_PATH,
) -> DataFrame:
    """Lit une table Silver Delta Lake.

    Args:
        spark: SparkSession active.
        entity: Nom de la table.
        silver_path: Chemin Silver.

    Returns:
        DataFrame Silver.
    """
    return spark.read.format("delta").load(f"{silver_path}/{entity}")


# ── Earned Premium ─────────────────────────────────────────────────────────────

def compute_earned_premium(policies_df: DataFrame) -> DataFrame:
    """Calcule la prime acquise (earned premium) au prorata de la période couverte.

    earned_premium = prime_annuelle × (jours_couverts_dans_période / 365)

    Utilise la fenêtre 2021-01-01 / 2024-12-31 comme période d'observation.

    Args:
        policies_df: DataFrame policies Silver.

    Returns:
        DataFrame avec colonnes earned_premium et période.
    """
    obs_start = F.to_date(F.lit("2021-01-01"))
    obs_end   = F.to_date(F.lit("2024-12-31"))

    return (
        policies_df
        .dropDuplicates(["policy_id"])
        .withColumn("effective_start",
                    F.greatest(F.col("date_souscription"), obs_start))
        .withColumn("effective_end",
                    F.least(F.col("date_echeance"), obs_end))
        .withColumn("days_covered",
                    F.greatest(
                        F.datediff(F.col("effective_end"), F.col("effective_start")),
                        F.lit(0)
                    ))
        .withColumn("earned_premium",
                    F.round(
                        F.col("prime_annuelle") * (F.col("days_covered") / F.lit(365.0)),
                        2
                    ))
        .withColumn("annee",
                    F.year(F.col("date_souscription")).cast("string"))
        .withColumn("trimestre",
                    F.concat(
                        F.year(F.col("date_souscription")).cast("string"),
                        F.lit("-Q"),
                        F.quarter(F.col("date_souscription")).cast("string")
                    ))
    )


# ── Incurred Losses ────────────────────────────────────────────────────────────

def compute_incurred_losses(claims_df: DataFrame) -> DataFrame:
    """Calcule les pertes encourues = paiements effectués + provisions ouvertes.

    incurred_losses = montant_indemnise (clos) + montant_provision (ouvert/en_cours)

    Args:
        claims_df: DataFrame claims Silver.

    Returns:
        DataFrame claims avec colonne incurred_loss par ligne.
    """
    return claims_df.withColumn(
        "incurred_loss",
        F.when(
            F.col("statut").isin(["clos"]),
            F.coalesce(F.col("montant_indemnise"), F.lit(0.0))
        ).when(
            F.col("statut").isin(["ouvert", "en_cours", "en_litige"]),
            F.coalesce(F.col("montant_provision"), F.lit(0.0))
        ).otherwise(F.lit(0.0))
    )


# ── Agrégation Gold ────────────────────────────────────────────────────────────

def aggregate_gold(
    claims_df: DataFrame,
    policies_ep_df: DataFrame,
) -> DataFrame:
    """Agrège sinistres et primes acquises par produit × région × canal × trimestre.

    Args:
        claims_df: DataFrame claims Silver avec incurred_loss.
        policies_ep_df: DataFrame policies avec earned_premium.

    Returns:
        DataFrame Gold agrégé.
    """
    claims_agg = (
        claims_df
        .withColumn("annee",     F.col("annee_sinistre"))
        .withColumn("trimestre", F.col("trimestre"))
        .groupBy("produit", "region_code", "canal_vente", "annee", "trimestre")
        .agg(
            F.count("claim_id").alias("nb_sinistres"),
            F.sum("incurred_loss").alias("incurred_losses"),
            F.sum("montant_declare").alias("montant_declare_total"),
            F.sum("montant_provision").alias("provisions_ouvertes"),
            F.countDistinct("client_id").alias("nb_clients_sinistres"),
            F.avg("delai_declaration_jours").alias("delai_moyen_declaration"),
            F.sum(F.when(F.col("statut") == "refuse", 1).otherwise(0)).alias("nb_refuses"),
        )
    )

    primes_agg = (
        policies_ep_df
        .groupBy("produit", "region_code", "canal_vente", "annee", "trimestre")
        .agg(
            F.sum("earned_premium").alias("earned_premium"),
            F.sum("prime_annuelle").alias("prime_annuelle_total"),
            F.countDistinct("policy_id").alias("nb_polices"),
            F.avg("prime_annuelle").alias("prime_moyenne"),
        )
    )

    gold = (
        primes_agg
        .join(claims_agg, on=["produit", "region_code", "canal_vente", "annee", "trimestre"], how="left")
        .fillna(0.0, subset=["incurred_losses", "nb_sinistres", "provisions_ouvertes"])
        .withColumn(
            "loss_ratio",
            F.when(F.col("earned_premium") > 0,
                   F.round(F.col("incurred_losses") / F.col("earned_premium") * 100, 4))
            .otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn("gold_computed_at", F.current_timestamp())
    )

    return gold


# ── Écriture Gold ──────────────────────────────────────────────────────────────

def write_gold(
    df: DataFrame,
    table_name: str,
    gold_path: str = GOLD_PATH,
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
) -> int:
    """Écrit une table Gold Delta Lake optimisée pour la lecture analytique.

    Args:
        df: DataFrame à écrire.
        table_name: Nom de la table Gold.
        gold_path: Chemin Gold.
        mode: Mode d'écriture.
        partition_cols: Colonnes de partitionnement.

    Returns:
        Nombre de lignes écrites.
    """
    target = f"{gold_path}/{table_name}"
    n = df.count()
    w = (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
        # Z-order hint via metadata (Databricks) — ignoré en local
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact",   "true")
    )
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.save(target)
    return n


# ── Vues YTD et rolling 12M ────────────────────────────────────────────────────

def compute_ytd_rolling(df: DataFrame) -> DataFrame:
    """Ajoute les métriques YTD et rolling 12 mois au Gold agrégé.

    Args:
        df: DataFrame Gold trimestriel.

    Returns:
        DataFrame enrichi avec métriques temporelles.
    """
    from pyspark.sql import Window as W

    w_ytd = (
        W.partitionBy("produit", "region_code", "canal_vente", "annee")
        .orderBy("trimestre")
        .rowsBetween(W.unboundedPreceding, W.currentRow)
    )

    w_roll12 = (
        W.partitionBy("produit", "region_code", "canal_vente")
        .orderBy("trimestre")
        .rowsBetween(-3, W.currentRow)  # 4 trimestres = 12 mois glissants
    )

    return (
        df
        .withColumn("ytd_incurred_losses",  F.sum("incurred_losses").over(w_ytd))
        .withColumn("ytd_earned_premium",   F.sum("earned_premium").over(w_ytd))
        .withColumn("ytd_loss_ratio",
                    F.when(F.col("ytd_earned_premium") > 0,
                           F.round(F.col("ytd_incurred_losses") / F.col("ytd_earned_premium") * 100, 4))
                    .otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("rolling_12m_incurred", F.sum("incurred_losses").over(w_roll12))
        .withColumn("rolling_12m_premium",  F.sum("earned_premium").over(w_roll12))
        .withColumn("rolling_12m_loss_ratio",
                    F.when(F.col("rolling_12m_premium") > 0,
                           F.round(F.col("rolling_12m_incurred") / F.col("rolling_12m_premium") * 100, 4))
                    .otherwise(F.lit(None).cast(DoubleType())))
    )


# ── Pipeline principal ─────────────────────────────────────────────────────────

def run_silver_to_gold(
    spark: Optional[SparkSession] = None,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    batch_id: Optional[str] = None,
) -> dict[str, int]:
    """Exécute la transformation complète Silver → Gold.

    Args:
        spark: SparkSession (créée si None).
        silver_path: Chemin Silver source.
        gold_path: Chemin Gold cible.
        batch_id: Identifiant batch.

    Returns:
        Dictionnaire avec volumétrie Gold.
    """
    _spark = spark or get_spark_session()
    _batch_id = batch_id or str(uuid.uuid4())[:8]
    results: dict[str, int] = {}

    with pipeline_step("silver_to_gold", batch_id=_batch_id) as log:

        log.info("Reading Silver tables")
        claims_df   = read_silver(_spark, "claims",   silver_path)
        policies_df = read_silver(_spark, "policies", silver_path)

        log.info("Computing earned premiums")
        policies_ep = compute_earned_premium(policies_df)

        log.info("Computing incurred losses")
        claims_il = compute_incurred_losses(claims_df)

        log.info("Aggregating Gold")
        gold_df = aggregate_gold(claims_il, policies_ep)
        gold_df = compute_ytd_rolling(gold_df)

        log.info("Writing Gold tables")
        results["portfolio_kpis"] = write_gold(
            gold_df,
            "portfolio_kpis",
            gold_path,
            partition_cols=["annee", "trimestre", "produit"],
        )

        # Table earned premium (pour réconciliation Silver vs Gold)
        results["earned_premium"] = write_gold(
            policies_ep.select("policy_id", "produit", "earned_premium",
                               "annee", "trimestre", "region_code", "canal_vente"),
            "earned_premium",
            gold_path,
        )

        log.complete(nb_records_out=results["portfolio_kpis"])

    return results


if __name__ == "__main__":
    stats = run_silver_to_gold()
    print(f"Silver → Gold terminé : {stats}")
