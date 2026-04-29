"""
Tests qualité couche Bronze avec pytest + Great Expectations.

Vérifie :
- Absence de doublons sur claim_id
- Dates dans plage valide (2020-2025)
- Montants > 0
- Taux de nulls < 2 % sur colonnes critiques
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.utils.config import BRONZE_PATH, QUALITY_CONFIG
from src.utils.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """SparkSession partagée pour tous les tests Bronze."""
    return get_spark_session(app_name="test_bronze")


@pytest.fixture(scope="module")
def bronze_claims(spark: SparkSession):
    """Charge la table Bronze claims."""
    return spark.read.format("delta").load(f"{BRONZE_PATH}/claims")


@pytest.fixture(scope="module")
def bronze_policies(spark: SparkSession):
    """Charge la table Bronze policies."""
    return spark.read.format("delta").load(f"{BRONZE_PATH}/policies")


# ── Claims Bronze ──────────────────────────────────────────────────────────────

class TestBronzeClaims:
    """Tests qualité sur la table Bronze claims."""

    def test_no_duplicate_claim_id(self, bronze_claims):
        """Vérifie l'unicité de claim_id en Bronze."""
        from pyspark.sql import functions as F
        n_total = bronze_claims.count()
        n_distinct = bronze_claims.select("claim_id").distinct().count()
        assert n_distinct == n_total, (
            f"Doublons détectés : {n_total - n_distinct} sur {n_total} lignes"
        )

    def test_dates_valid_range(self, bronze_claims):
        """Vérifie que les dates sont dans la plage 2020-2025."""
        from pyspark.sql import functions as F
        out_of_range = (
            bronze_claims
            .filter(
                (F.col("date_sinistre") < QUALITY_CONFIG.claim_date_min) |
                (F.col("date_sinistre") > QUALITY_CONFIG.claim_date_max)
            )
            .count()
        )
        assert out_of_range == 0, (
            f"{out_of_range} sinistres hors plage {QUALITY_CONFIG.claim_date_min}"
            f"–{QUALITY_CONFIG.claim_date_max}"
        )

    def test_montant_positive(self, bronze_claims):
        """Vérifie que montant_declare est strictement positif."""
        from pyspark.sql import functions as F
        n_negative = (
            bronze_claims
            .filter(
                (F.col("montant_declare").isNotNull()) &
                (F.col("montant_declare") <= 0)
            )
            .count()
        )
        assert n_negative == 0, f"{n_negative} sinistres avec montant_declare ≤ 0"

    def test_critical_columns_null_rate(self, bronze_claims):
        """Vérifie que les colonnes critiques ont un taux de nulls < 2 %."""
        from pyspark.sql import functions as F
        critical_cols = ["claim_id", "policy_id", "client_id",
                         "date_sinistre", "date_declaration", "statut"]
        n_total = bronze_claims.count()
        if n_total == 0:
            pytest.skip("Table Bronze vide")

        for col in critical_cols:
            if col not in bronze_claims.columns:
                continue
            n_nulls = bronze_claims.filter(F.col(col).isNull()).count()
            null_rate = n_nulls / n_total
            assert null_rate < QUALITY_CONFIG.max_null_rate_critical, (
                f"Colonne '{col}' : taux de nulls {null_rate:.2%} "
                f"(seuil : {QUALITY_CONFIG.max_null_rate_critical:.2%})"
            )

    def test_ingestion_metadata_present(self, bronze_claims):
        """Vérifie la présence des colonnes métadonnées d'ingestion."""
        required_meta = ["ingestion_timestamp", "source_system", "batch_id"]
        for col in required_meta:
            assert col in bronze_claims.columns, f"Colonne métadata manquante : {col}"

    def test_statut_valid_values(self, bronze_claims):
        """Vérifie que statut ne contient que des valeurs attendues."""
        from pyspark.sql import functions as F
        valid_statuts = {"ouvert", "en_cours", "clos", "refuse", "en_litige", "inconnu"}
        invalid = (
            bronze_claims
            .filter(F.col("statut").isNotNull())
            .filter(~F.col("statut").isin(list(valid_statuts)))
            .count()
        )
        assert invalid == 0, f"{invalid} lignes avec statut invalide"


# ── Policies Bronze ────────────────────────────────────────────────────────────

class TestBronzePolicies:
    """Tests qualité sur la table Bronze policies."""

    def test_policy_id_not_null(self, bronze_policies):
        """Vérifie que policy_id n'est jamais nul."""
        from pyspark.sql import functions as F
        n_null = bronze_policies.filter(F.col("policy_id").isNull()).count()
        assert n_null == 0, f"{n_null} polices sans policy_id"

    def test_prime_positive(self, bronze_policies):
        """Vérifie que prime_annuelle est positive."""
        from pyspark.sql import functions as F
        n_bad = (
            bronze_policies
            .filter(
                F.col("prime_annuelle").isNotNull() &
                (F.col("prime_annuelle") <= 0)
            )
            .count()
        )
        assert n_bad == 0, f"{n_bad} polices avec prime_annuelle ≤ 0"

    def test_produit_valid_values(self, bronze_policies):
        """Vérifie que produit est dans la liste attendue."""
        from pyspark.sql import functions as F
        valid = {"auto", "habitation", "sante", "vie", "mrh", "inconnu"}
        invalid = (
            bronze_policies
            .filter(F.col("produit").isNotNull())
            .filter(~F.col("produit").isin(list(valid)))
            .count()
        )
        assert invalid == 0, f"{invalid} polices avec produit invalide"

    def test_dates_coherence(self, bronze_policies):
        """Vérifie que date_echeance > date_souscription."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import DateType
        n_bad = (
            bronze_policies
            .filter(
                F.col("date_souscription").isNotNull() &
                F.col("date_echeance").isNotNull()
            )
            .filter(
                F.to_date(F.col("date_echeance")) <= F.to_date(F.col("date_souscription"))
            )
            .count()
        )
        assert n_bad == 0, f"{n_bad} polices avec date_echeance ≤ date_souscription"


# ── Great Expectations — bronze claims (standalone check) ─────────────────────

def run_ge_bronze_checks(spark: SparkSession) -> dict[str, bool]:
    """Exécute les checks Great Expectations sur Bronze claims.

    Args:
        spark: SparkSession active.

    Returns:
        Dictionnaire {nom_check: passed}.
    """
    from pyspark.sql import functions as F
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/claims")
    n = df.count()
    if n == 0:
        return {"empty_table": False}

    results: dict[str, bool] = {}

    # Check 1 : unicité claim_id
    results["unique_claim_id"] = df.select("claim_id").distinct().count() == n

    # Check 2 : nulls < 2 %
    for col in ["claim_id", "policy_id", "client_id"]:
        null_rate = df.filter(F.col(col).isNull()).count() / n
        results[f"null_rate_{col}"] = null_rate < 0.02

    # Check 3 : montants > 0
    results["positive_amounts"] = (
        df.filter((F.col("montant_declare").isNotNull()) & (F.col("montant_declare") <= 0)).count() == 0
    )

    return results


if __name__ == "__main__":
    sp = get_spark_session(app_name="test_bronze_standalone")
    checks = run_ge_bronze_checks(sp)
    for name, passed in checks.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status} — {name}")
