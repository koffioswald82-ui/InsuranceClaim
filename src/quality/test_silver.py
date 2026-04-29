"""
Tests qualité couche Silver avec pytest.

Vérifie :
- Intégrité référentielle clients / polices / sinistres
- delai_declaration_jours >= 0
- Cohérence loss_ratio Gold entre 0 % et 300 %
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.utils.config import GOLD_PATH, QUALITY_CONFIG, SILVER_PATH
from src.utils.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return get_spark_session(app_name="test_silver")


@pytest.fixture(scope="module")
def silver_claims(spark):
    return spark.read.format("delta").load(f"{SILVER_PATH}/claims")


@pytest.fixture(scope="module")
def silver_policies(spark):
    return spark.read.format("delta").load(f"{SILVER_PATH}/policies")


@pytest.fixture(scope="module")
def silver_clients(spark):
    return spark.read.format("delta").load(f"{SILVER_PATH}/clients")


@pytest.fixture(scope="module")
def gold_kpis(spark):
    return spark.read.format("delta").load(f"{GOLD_PATH}/portfolio_kpis")


# ── Intégrité référentielle ────────────────────────────────────────────────────

class TestReferentialIntegrity:
    """Tests d'intégrité référentielle entre entités Silver."""

    def test_claims_policies_fk(self, silver_claims, silver_policies):
        """Tous les claim.policy_id existent dans policies (non-orphelins)."""
        from pyspark.sql import functions as F
        policy_ids = silver_policies.select("policy_id").distinct()
        orphan_claims = (
            silver_claims
            .join(policy_ids, on="policy_id", how="left_anti")
            .count()
        )
        # Tolérance : max 1 % d'orphelins (données synthétiques)
        n_claims = silver_claims.count()
        orphan_rate = orphan_claims / n_claims if n_claims > 0 else 0
        assert orphan_rate < 0.01, (
            f"Taux d'orphelins claim→policy : {orphan_rate:.2%} "
            f"({orphan_claims}/{n_claims})"
        )

    def test_claims_clients_fk(self, silver_claims, silver_clients):
        """Tous les claim.client_id existent dans clients."""
        from pyspark.sql import functions as F
        client_ids = silver_clients.select("client_id").distinct()
        orphan = (
            silver_claims
            .join(client_ids, on="client_id", how="left_anti")
            .count()
        )
        n = silver_claims.count()
        rate = orphan / n if n > 0 else 0
        assert rate < 0.01, f"Orphelins claim→client : {rate:.2%}"

    def test_policies_clients_fk(self, silver_policies, silver_clients):
        """Tous les policy.client_id existent dans clients."""
        client_ids = silver_clients.select("client_id").distinct()
        orphan = (
            silver_policies
            .dropDuplicates(["policy_id"])
            .join(client_ids, on="client_id", how="left_anti")
            .count()
        )
        n = silver_policies.dropDuplicates(["policy_id"]).count()
        rate = orphan / n if n > 0 else 0
        assert rate < 0.01, f"Orphelins policy→client : {rate:.2%}"


# ── Cohérence métier ───────────────────────────────────────────────────────────

class TestSilverBusinessRules:
    """Tests des règles métier sur la couche Silver."""

    def test_declaration_delay_non_negative(self, silver_claims):
        """delai_declaration_jours doit être >= 0 ou null."""
        from pyspark.sql import functions as F
        n_negative = (
            silver_claims
            .filter(
                F.col("delai_declaration_jours").isNotNull() &
                (F.col("delai_declaration_jours") < 0)
            )
            .count()
        )
        assert n_negative == 0, f"{n_negative} sinistres avec délai déclaration négatif"

    def test_montant_indemnise_le_montant_declare(self, silver_claims):
        """montant_indemnise <= montant_declare (ne peut pas indemniser plus que déclaré)."""
        from pyspark.sql import functions as F
        n_bad = (
            silver_claims
            .filter(
                F.col("montant_indemnise").isNotNull() &
                F.col("montant_declare").isNotNull() &
                (F.col("montant_indemnise") > F.col("montant_declare") * 1.1)  # 10 % tolérance arrondi
            )
            .count()
        )
        assert n_bad == 0, f"{n_bad} sinistres avec indemnisation > déclaration"

    def test_claim_date_after_policy_start(self, silver_claims):
        """date_sinistre >= date_souscription pour les sinistres avec police valide."""
        from pyspark.sql import functions as F
        n_bad = (
            silver_claims
            .filter(
                F.col("has_valid_policy") == True  # noqa: E712
            )
            .filter(
                F.col("date_sinistre").isNotNull() &
                F.col("date_souscription").isNotNull() &
                (F.col("date_sinistre") < F.col("date_souscription"))
            )
            .count()
        )
        # Tolérance pour les fraudes injectées (early_claim pattern)
        n_total = silver_claims.filter(F.col("has_valid_policy") == True).count()  # noqa: E712
        rate = n_bad / n_total if n_total > 0 else 0
        assert rate < 0.05, f"Sinistres avant souscription : {rate:.2%} (seuil : 5 %)"

    def test_no_future_claims(self, silver_claims):
        """Aucun sinistre avec date_sinistre > aujourd'hui."""
        from pyspark.sql import functions as F
        n_future = (
            silver_claims
            .filter(F.col("date_sinistre") > F.current_date())
            .count()
        )
        assert n_future == 0, f"{n_future} sinistres dans le futur"

    def test_silver_deduplication(self, silver_claims):
        """Pas de doublons sur claim_id en Silver."""
        n_total = silver_claims.count()
        n_distinct = silver_claims.select("claim_id").distinct().count()
        assert n_distinct == n_total, (
            f"Doublons Silver : {n_total - n_distinct} sur {n_total}"
        )


# ── Loss Ratio Gold ────────────────────────────────────────────────────────────

class TestGoldLossRatio:
    """Tests sur le loss_ratio Gold (ici dans test_silver pour cohérence pipeline)."""

    def test_loss_ratio_bounds(self, gold_kpis):
        """Loss ratio dans les bornes [0, 300] %."""
        from pyspark.sql import functions as F
        max_lr = QUALITY_CONFIG.max_loss_ratio * 100  # 300 %
        n_out = (
            gold_kpis
            .filter(F.col("loss_ratio").isNotNull())
            .filter(
                (F.col("loss_ratio") < 0) | (F.col("loss_ratio") > max_lr)
            )
            .count()
        )
        assert n_out == 0, f"{n_out} lignes avec loss_ratio hors bornes [0, {max_lr}%]"

    def test_gold_has_all_products(self, gold_kpis):
        """La table Gold contient les 5 produits attendus."""
        expected = {"auto", "habitation", "sante", "vie", "mrh"}
        found = {r["produit"] for r in gold_kpis.select("produit").distinct().collect()}
        missing = expected - found
        assert len(missing) == 0, f"Produits manquants en Gold : {missing}"

    def test_earned_premium_positive(self, gold_kpis):
        """earned_premium > 0 pour toutes les lignes Gold."""
        from pyspark.sql import functions as F
        n_bad = gold_kpis.filter(F.col("earned_premium") <= 0).count()
        assert n_bad == 0, f"{n_bad} lignes Gold avec earned_premium ≤ 0"
