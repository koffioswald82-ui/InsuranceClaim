.PHONY: install generate run test lint quality clean help

# ── Variables ──────────────────────────────────────────────────────────────────
PYTHON      := python
PIP         := pip
PYTEST      := pytest
BLACK       := black
FLAKE8      := flake8
COVERAGE    := coverage
SRC_DIRS    := src/ generate_data.py monitoring/

# ── Aide ───────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "AXA Claims Lakehouse — Commandes disponibles"
	@echo "============================================="
	@echo "  make install   → pip install -r requirements.txt"
	@echo "  make generate  → Génère les données synthétiques (50k clients, etc.)"
	@echo "  make run       → Pipeline complet Bronze → Silver → Gold"
	@echo "  make test      → pytest + rapport de couverture"
	@echo "  make lint      → black + flake8"
	@echo "  make quality   → Génère data_quality_report.html"
	@echo "  make clean     → Supprime données générées et cache Spark"
	@echo ""

# ── Installation ───────────────────────────────────────────────────────────────
install:
	@echo "[install] Installation des dépendances..."
	$(PIP) install -r requirements.txt
	@echo "[install] OK"

# ── Génération des données ─────────────────────────────────────────────────────
generate:
	@echo "[generate] Génération des données synthétiques..."
	$(PYTHON) generate_data.py
	@echo "[generate] OK — voir data/raw/ et data/external/"

# ── Pipeline complet ───────────────────────────────────────────────────────────
run:
	@echo "[run] Pipeline Bronze → Silver → Gold..."
	$(PYTHON) -c "\
from src.utils.spark_session import get_spark_session; \
from src.ingestion.ingest_claims import ingest_claims; \
from src.ingestion.ingest_policies import ingest_policies; \
from src.transformation.bronze_to_silver import run_bronze_to_silver; \
from src.transformation.silver_to_gold import run_silver_to_gold; \
from src.transformation.fraud_flags import run_fraud_flags; \
from src.business_views.churn_score import compute_churn_scores; \
from src.business_views.loss_ratio import compute_loss_ratio; \
from src.business_views.claims_aging import compute_claims_aging; \
from src.business_views.portfolio_exposure import compute_portfolio_exposure; \
spark = get_spark_session(); \
print('--- Ingestion ---'); \
ingest_claims(spark=spark); \
ingest_policies(spark=spark); \
print('--- Bronze → Silver ---'); \
run_bronze_to_silver(spark=spark); \
print('--- Silver → Gold ---'); \
run_silver_to_gold(spark=spark); \
print('--- Fraud Detection ---'); \
run_fraud_flags(spark=spark); \
print('--- Business Views ---'); \
compute_churn_scores(spark=spark); \
compute_loss_ratio(spark=spark); \
compute_claims_aging(spark=spark); \
compute_portfolio_exposure(spark=spark); \
print('Pipeline terminé.'); \
"
	@echo "[run] OK"

# ── Tests ──────────────────────────────────────────────────────────────────────
test:
	@echo "[test] Exécution des tests..."
	$(PYTEST) src/quality/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:reports/coverage_html \
		--cov-fail-under=60 \
		-v \
		--tb=short
	@echo "[test] Rapport de couverture : reports/coverage_html/index.html"

# ── Lint ───────────────────────────────────────────────────────────────────────
lint:
	@echo "[lint] black format check..."
	$(BLACK) --check --diff $(SRC_DIRS)
	@echo "[lint] flake8..."
	$(FLAKE8) $(SRC_DIRS) \
		--max-line-length=100 \
		--extend-ignore=E203,W503,D100,D101,D102,D103,D104 \
		--exclude=__pycache__,*.egg-info,.git
	@echo "[lint] OK"

lint-fix:
	@echo "[lint-fix] Reformatage avec black..."
	$(BLACK) $(SRC_DIRS)

# ── Rapport qualité ────────────────────────────────────────────────────────────
quality:
	@echo "[quality] Génération du rapport HTML de qualité..."
	$(PYTHON) monitoring/data_quality_report.py
	@echo "[quality] Rapport : reports/data_quality_report.html"

# ── Nettoyage ──────────────────────────────────────────────────────────────────
clean:
	@echo "[clean] Suppression des données générées et du cache Spark..."
	rm -rf data/raw/*.csv data/raw/*.json data/raw/*.parquet
	rm -rf data/external/*.csv
	rm -rf lake/
	rm -rf spark-warehouse/
	rm -rf metastore_db/
	rm -rf derby.log
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf reports/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	@echo "[clean] OK"

# ── Pre-commit ────────────────────────────────────────────────────────────────
pre-commit-install:
	pre-commit install

pre-commit-run:
	pre-commit run --all-files
