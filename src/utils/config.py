"""
Configuration centralisée du pipeline AXA Claims Lakehouse.

Toutes les valeurs sont lues depuis les variables d'environnement
(avec valeurs par défaut pour le mode local/développement).
Aucune credential ne doit être hardcodée ici.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


# ── Paths ──────────────────────────────────────────────────────────────────────

BASE_DIR = Path(os.getenv("AXA_BASE_DIR", str(Path(__file__).parent.parent.parent)))
DATA_DIR = BASE_DIR / "data"

# Simule ADLS Gen2 en local
LAKE_ROOT = Path(os.getenv("AXA_LAKE_ROOT", str(BASE_DIR / "lake")))
BRONZE_PATH = str(LAKE_ROOT / "bronze")
SILVER_PATH = str(LAKE_ROOT / "silver")
GOLD_PATH   = str(LAKE_ROOT / "gold")

RAW_DIR      = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
DEAD_LETTER  = str(LAKE_ROOT / "dead_letter_queue")

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ── Environment ────────────────────────────────────────────────────────────────

ENV = os.getenv("AXA_ENV", "local")           # local | dev | staging | prod
LOG_LEVEL = os.getenv("AXA_LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv("AXA_LOG_FORMAT", "json")  # json | text


# ── Spark ──────────────────────────────────────────────────────────────────────

SPARK_MASTER = os.getenv("AXA_SPARK_MASTER", "local[2]")
SPARK_APP_NAME = os.getenv("AXA_SPARK_APP_NAME", "axa-claims-lakehouse")
SPARK_DRIVER_MEMORY = os.getenv("AXA_SPARK_DRIVER_MEMORY", "768m")
SPARK_EXECUTOR_MEMORY = os.getenv("AXA_SPARK_EXECUTOR_MEMORY", "768m")
DELTA_VERSION = os.getenv("AXA_DELTA_VERSION", "3.1.0")


# ── Source files ───────────────────────────────────────────────────────────────

CLAIMS_CSV    = str(RAW_DIR / "claims.csv")
POLICIES_JSON = str(RAW_DIR / "policies.json")
CLIENTS_PARQUET = str(RAW_DIR / "clients.parquet")
PAYMENTS_CSV  = str(RAW_DIR / "payments.csv")
INSEE_CSV     = str(EXTERNAL_DIR / "insee_regions.csv")
INFLATION_CSV = str(EXTERNAL_DIR / "inflation_rates.csv")


# ── Fraud detection thresholds ─────────────────────────────────────────────────

@dataclass
class FraudConfig:
    """Paramètres du moteur de règles anti-fraude."""
    early_claim_hours: int = int(os.getenv("FRAUD_EARLY_CLAIM_HOURS", "48"))
    high_amount_stddev_multiplier: float = float(os.getenv("FRAUD_HIGH_AMOUNT_STDDEV", "3.0"))
    frequent_claimant_count: int = int(os.getenv("FRAUD_FREQUENT_CLAIMANT_COUNT", "3"))
    frequent_claimant_window_days: int = int(os.getenv("FRAUD_FREQUENT_CLAIMANT_WINDOW", "365"))
    address_cluster_days: int = int(os.getenv("FRAUD_ADDRESS_CLUSTER_DAYS", "30"))
    late_declaration_days: int = int(os.getenv("FRAUD_LATE_DECLARATION_DAYS", "180"))

    rule_weights: dict[str, int] = field(default_factory=lambda: {
        "RULE_001_early_claim":        25,
        "RULE_002_high_amount":        30,
        "RULE_003_frequent_claimant":  20,
        "RULE_004_address_cluster":    15,
        "RULE_005_late_declaration":   10,
    })


# ── Churn score weights ────────────────────────────────────────────────────────

@dataclass
class ChurnConfig:
    """Poids du score de churn client."""
    weight_seniority_inverse: float = float(os.getenv("CHURN_W_SENIORITY", "0.30"))
    weight_unpaid:            float = float(os.getenv("CHURN_W_UNPAID",    "0.30"))
    weight_refused_claims:    float = float(os.getenv("CHURN_W_REFUSED",   "0.20"))
    weight_mono_contract:     float = float(os.getenv("CHURN_W_MONO",      "0.20"))
    max_seniority_years: int = 10


# ── Data quality thresholds ────────────────────────────────────────────────────

@dataclass
class QualityConfig:
    """Seuils de qualité des données."""
    max_null_rate_critical: float = float(os.getenv("QA_MAX_NULL_RATE", "0.02"))
    claim_date_min: str = os.getenv("QA_CLAIM_DATE_MIN", "2020-01-01")
    claim_date_max: str = os.getenv("QA_CLAIM_DATE_MAX", "2025-12-31")
    max_loss_ratio:  float = float(os.getenv("QA_MAX_LOSS_RATIO", "3.0"))


# ── Singletons ─────────────────────────────────────────────────────────────────

FRAUD_CONFIG   = FraudConfig()
CHURN_CONFIG   = ChurnConfig()
QUALITY_CONFIG = QualityConfig()

# ── Partitioning ───────────────────────────────────────────────────────────────

BRONZE_PARTITION_COLS = ["annee_ingestion", "mois_ingestion"]
SILVER_PARTITION_COLS = ["annee_sinistre", "mois_sinistre"]
GOLD_PARTITION_COLS   = ["annee", "trimestre", "produit"]
