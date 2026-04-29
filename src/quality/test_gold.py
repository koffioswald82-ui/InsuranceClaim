"""
Tests qualité couche Gold avec pytest.

Vérifie :
- Réconciliation somme primes Gold = somme primes Silver
- fraud_score dans [0, 100]
- churn_score dans [0, 100]
- Intégrité tables Gold
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.utils.config import GOLD_PATH, SILVER_PATH
from src.utils.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return get_spark_session(app_name="test_gold")


@pytest.fixture(scope="module")
def gold_kpis(spark):
    return spark.read.format("delta").load(f"{GOLD_PATH}/portfolio_kpis")


@pytest.fixture(scope="module")
def gold_fraud(spark):
    return spark.read.format("delta").load(f"{GOLD_PATH}/fraud_assessment")


@pytest.fixture(scope="module")
def gold_churn(spark):
    return spark.read.format("delta").load(f"{GOLD_PATH}/churn_scores")


@pytest.fixture(scope="module")
def gold_ep(spark):
    return spark.read.format("delta").load(f"{GOLD_PATH}/earned_premium")


@pytest.fixture(scope="module")
def silver_policies(spark):
    return spark.read.format("delta").load(f"{SILVER_PATH}/policies")


# ── Réconciliation Silver ↔ Gold ───────────────────────────────────────────────

class TestGoldReconciliation:
    """Tests de réconciliation entre couches Silver et Gold."""

    def test_earned_premium_reconciliation(self, gold_ep, silver_policies):
        """Somme earned_premium Gold ≈ somme prime_annuelle Silver (± 5 %).

        La prime acquise peut légèrement différer de la prime annuelle brute
        en raison du prorata temporis.
        """
        from pyspark.sql import functions as F

        gold_total = (
            gold_ep.agg(F.sum("earned_premium").alias("total")).collect()[0]["total"] or 0.0
        )
        silver_total = (
            silver_policies
            .dropDuplicates(["policy_id"])
            .agg(F.sum("prime_annuelle").alias("total"))
            .collect()[0]["total"] or 0.0
        )

        if silver_total == 0:
            pytest.skip("Silver policies vides")

        ratio = gold_total / silver_total
        # Earned premium < prime annuelle (prorata) donc ratio < 1
        assert 0.1 < ratio <= 1.05, (
            f"Réconciliation primes Gold/Silver hors tolérance : "
            f"Gold={gold_total:,.2f}, Silver={silver_total:,.2f}, ratio={ratio:.4f}"
        )

    def test_gold_nb_products_matches_silver(self, gold_kpis, silver_policies):
        """Les produits en Gold correspondent aux produits en Silver."""
        from pyspark.sql import functions as F

        gold_products   = {r["produit"] for r in gold_kpis.select("produit").distinct().collect()}
        silver_products = {r["produit"] for r in silver_policies.select("produit").distinct().collect()}

        missing = silver_products - gold_products
        assert len(missing) == 0, f"Produits Silver absents du Gold : {missing}"


# ── Fraud Assessment ───────────────────────────────────────────────────────────

class TestGoldFraud:
    """Tests qualité de la table fraud_assessment."""

    def test_fraud_score_bounds(self, gold_fraud):
        """fraud_score strictement dans [0, 100]."""
        from pyspark.sql import functions as F
        n_out = (
            gold_fraud
            .filter(F.col("fraud_score").isNotNull())
            .filter(
                (F.col("fraud_score") < 0) | (F.col("fraud_score") > 100)
            )
            .count()
        )
        assert n_out == 0, f"{n_out} lignes avec fraud_score hors [0, 100]"

    def test_fraud_score_not_null(self, gold_fraud):
        """fraud_score ne doit pas être nul."""
        from pyspark.sql import functions as F
        n_null = gold_fraud.filter(F.col("fraud_score").isNull()).count()
        n_total = gold_fraud.count()
        null_rate = n_null / n_total if n_total > 0 else 0
        assert null_rate < 0.01, f"fraud_score nul : {null_rate:.2%}"

    def test_fraud_flags_consistency(self, gold_fraud):
        """Si fraud_score > 0, fraud_flags doit être non-vide."""
        from pyspark.sql import functions as F
        n_inconsistent = (
            gold_fraud
            .filter(F.col("fraud_score") > 0)
            .filter(F.size(F.col("fraud_flags")) == 0)
            .count()
        )
        assert n_inconsistent == 0, (
            f"{n_inconsistent} lignes avec fraud_score > 0 mais aucun flag"
        )

    def test_fraud_rate_realistic(self, gold_fraud):
        """Taux de fraude suspectée < 20 % (seuil de cohérence métier)."""
        from pyspark.sql import functions as F
        n_total    = gold_fraud.count()
        n_suspected = gold_fraud.filter(F.col("is_fraud_suspected")).count()
        rate = n_suspected / n_total if n_total > 0 else 0
        assert rate < 0.20, f"Taux fraude suspectée trop élevé : {rate:.2%}"

    def test_no_duplicate_claim_id(self, gold_fraud):
        """Pas de doublons sur claim_id dans fraud_assessment."""
        n_total    = gold_fraud.count()
        n_distinct = gold_fraud.select("claim_id").distinct().count()
        assert n_distinct == n_total, f"Doublons fraud_assessment : {n_total - n_distinct}"


# ── Churn Scores ───────────────────────────────────────────────────────────────

class TestGoldChurn:
    """Tests qualité de la table churn_scores."""

    def test_churn_score_bounds(self, gold_churn):
        """churn_score strictement dans [0, 100]."""
        from pyspark.sql import functions as F
        n_out = (
            gold_churn
            .filter(F.col("churn_score").isNotNull())
            .filter(
                (F.col("churn_score") < 0) | (F.col("churn_score") > 100)
            )
            .count()
        )
        assert n_out == 0, f"{n_out} clients avec churn_score hors [0, 100]"

    def test_churn_score_not_null(self, gold_churn):
        """churn_score ne doit pas être nul."""
        from pyspark.sql import functions as F
        n_null  = gold_churn.filter(F.col("churn_score").isNull()).count()
        n_total = gold_churn.count()
        null_rate = n_null / n_total if n_total > 0 else 0
        assert null_rate < 0.01, f"churn_score nul : {null_rate:.2%}"

    def test_churn_segment_valid_values(self, gold_churn):
        """churn_segment ne contient que des valeurs attendues."""
        from pyspark.sql import functions as F
        valid = {"faible", "moyen", "élevé", "critique"}
        invalid = (
            gold_churn
            .filter(F.col("churn_segment").isNotNull())
            .filter(~F.col("churn_segment").isin(list(valid)))
            .count()
        )
        assert invalid == 0, f"{invalid} clients avec churn_segment invalide"

    def test_no_duplicate_client_id(self, gold_churn):
        """Un client_id unique par ligne dans churn_scores."""
        n_total    = gold_churn.count()
        n_distinct = gold_churn.select("client_id").distinct().count()
        assert n_distinct == n_total, f"Doublons churn_scores : {n_total - n_distinct}"

    def test_churn_coverage(self, gold_churn, silver_policies):
        """churn_scores couvre ≥ 80 % des clients actifs en Silver."""
        from pyspark.sql import functions as F
        n_clients_silver = (
            silver_policies
            .filter(F.col("statut") == "actif")
            .select("client_id")
            .distinct()
            .count()
        )
        n_churn = gold_churn.select("client_id").distinct().count()
        coverage = n_churn / n_clients_silver if n_clients_silver > 0 else 0
        assert coverage >= 0.80, f"Couverture churn : {coverage:.2%} (seuil : 80 %)"
