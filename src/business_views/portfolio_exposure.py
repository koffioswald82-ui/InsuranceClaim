"""
Business view : Portfolio Exposure & Concentration Risk.

Exposition totale par région + Herfindahl-Hirschman Index (HHI)
de concentration par produit.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.utils.config import EXTERNAL_DIR, GOLD_PATH, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("business_views.portfolio_exposure")


def compute_hhi(df: DataFrame, value_col: str, group_col: str) -> DataFrame:
    """Calcule l'indice HHI (Herfindahl-Hirschman Index) de concentration.

    HHI = Σ(market_share_i²) × 10000
    HHI < 1500 → marché peu concentré
    1500 ≤ HHI < 2500 → concentration modérée
    HHI ≥ 2500 → marché concentré

    Args:
        df: DataFrame avec colonnes value_col et group_col.
        value_col: Colonne de valeur (ex. earned_premium).
        group_col: Colonne de segmentation (ex. produit).

    Returns:
        DataFrame avec colonne hhi_{value_col}.
    """
    total = df.agg(F.sum(value_col).alias("total")).collect()[0]["total"] or 1.0

    hhi_df = (
        df
        .groupBy(group_col)
        .agg(F.sum(value_col).alias("segment_total"))
        .withColumn("market_share", F.col("segment_total") / F.lit(total))
        .withColumn("hhi_component", F.pow(F.col("market_share"), 2) * F.lit(10000.0))
    )

    hhi_score = hhi_df.agg(F.sum("hhi_component").alias("hhi")).collect()[0]["hhi"] or 0.0

    return df.withColumn(f"hhi_{value_col}", F.lit(round(hhi_score, 2)))


def compute_portfolio_exposure(
    spark: Optional[SparkSession] = None,
    silver_path: str = SILVER_PATH,
    gold_path: str = GOLD_PATH,
    output_path: Optional[str] = None,
) -> DataFrame:
    """Calcule l'exposition du portefeuille par région et le HHI par produit.

    Produit :
    - exposure_by_region : somme des montants garantis par région
    - concentration_risk : HHI par produit

    Args:
        spark: SparkSession active.
        silver_path: Chemin Silver.
        gold_path: Chemin Gold.
        output_path: Chemin de sortie Gold.

    Returns:
        DataFrame exposition par région.
    """
    _spark = spark or get_spark_session()
    _out = output_path or f"{gold_path}/portfolio_exposure"

    log.info("Loading Silver tables")
    policies = _spark.read.format("delta").load(f"{silver_path}/policies")
    claims   = _spark.read.format("delta").load(f"{silver_path}/claims")

    # Charger données INSEE pour enrichissement
    try:
        insee = _spark.read.option("header", "true").csv(str(EXTERNAL_DIR / "insee_regions.csv"))
    except Exception:
        insee = None
        log.warning("INSEE data not available, skipping enrichment")

    # ── Exposition par région ────────────────────────────────────────────────
    policies_dedup = policies.dropDuplicates(["policy_id"])

    region_exposure = (
        policies_dedup
        .filter(F.col("statut") == "actif")
        .groupBy("region_code", "produit", "canal_vente")
        .agg(
            F.count("policy_id").alias("nb_polices"),
            F.sum("prime_annuelle").alias("prime_totale"),
            F.sum("plafond_garantie").alias("exposition_max"),
            F.avg("prime_annuelle").alias("prime_moyenne"),
        )
    )

    # Enrichissement INSEE
    if insee is not None:
        insee_sel = (
            insee
            .withColumnRenamed("code", "region_code")
            .select("region_code", "nom", "population", "densite")
        )
        region_exposure = region_exposure.join(insee_sel, on="region_code", how="left")

    # ── Concentration risk (HHI) par produit ────────────────────────────────
    product_totals = (
        policies_dedup
        .filter(F.col("statut") == "actif")
        .groupBy("produit")
        .agg(F.sum("prime_annuelle").alias("prime_totale_produit"))
    )

    grand_total = (
        product_totals
        .agg(F.sum("prime_totale_produit").alias("gt"))
        .collect()[0]["gt"] or 1.0
    )

    concentration = (
        product_totals
        .withColumn("market_share_pct",
                    F.round(F.col("prime_totale_produit") / F.lit(grand_total) * 100, 4))
        .withColumn("hhi_component",
                    F.pow(F.col("prime_totale_produit") / F.lit(grand_total), 2) * F.lit(10000.0))
        .withColumn("hhi_category",
                    F.when(F.col("hhi_component") < 1500, F.lit("peu_concentré"))
                    .when(F.col("hhi_component") < 2500, F.lit("modéré"))
                    .otherwise(F.lit("concentré")))
    )

    # HHI global du portefeuille
    hhi_global = concentration.agg(F.sum("hhi_component").alias("hhi")).collect()[0]["hhi"] or 0.0

    log.info("Portfolio HHI global", hhi=round(hhi_global, 2))

    region_exposure = region_exposure.withColumn(
        "portfolio_hhi", F.lit(round(hhi_global, 2))
    )

    # ── Sinistres par région ─────────────────────────────────────────────────
    claims_by_region = (
        claims
        .groupBy("region_code")
        .agg(
            F.count("claim_id").alias("nb_sinistres"),
            F.sum("montant_declare").alias("sinistres_declares"),
            F.sum("montant_indemnise").alias("sinistres_indemnises"),
        )
    )

    final = (
        region_exposure
        .join(claims_by_region, on="region_code", how="left")
        .fillna(0.0, subset=["nb_sinistres", "sinistres_declares", "sinistres_indemnises"])
        .withColumn(
            "sinistres_per_policy",
            F.when(F.col("nb_polices") > 0,
                   F.round(F.col("nb_sinistres") / F.col("nb_polices"), 4))
            .otherwise(F.lit(0.0))
        )
        .withColumn("exposure_computed_at", F.current_timestamp())
    )

    # ── Écriture ──────────────────────────────────────────────────────────────
    (
        final.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(_out)
    )

    (
        concentration.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(f"{gold_path}/concentration_risk")
    )

    n = final.count()
    log.info("Portfolio exposure written", n_rows=n, hhi_global=round(hhi_global, 2))
    return final


if __name__ == "__main__":
    df = compute_portfolio_exposure()
    df.orderBy(F.col("exposition_max").desc()).show(20, truncate=False)
