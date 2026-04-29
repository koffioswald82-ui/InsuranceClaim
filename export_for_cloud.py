"""Exporte les tables Delta vers exports/ pour déploiement Streamlit Cloud.
Usage : python export_for_cloud.py
"""
import glob
import shutil
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
LAKE     = BASE_DIR / "lake"
EXPORTS  = BASE_DIR / "exports"
EXPORTS.mkdir(exist_ok=True)


def read_delta(rel_path: str) -> pd.DataFrame:
    files = [
        f for f in glob.glob(str(LAKE / rel_path / "**" / "*.parquet"), recursive=True)
        if "_delta_log" not in f
    ]
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


CLAIMS_COLS = [
    "claim_id", "policy_id", "type_sinistre", "date_sinistre",
    "montant_declare", "montant_indemnise", "prime_annuelle",
    "statut", "produit", "region_code", "delai_declaration_jours",
]
FRAUD_COLS = [
    "claim_id", "produit", "fraud_score", "is_fraud_suspected",
    "flag_early_claim", "flag_high_amount", "flag_frequent_claimant",
    "flag_address_cluster", "flag_late_declaration",
]
KPIS_COLS = [
    "region_code", "canal_vente", "nb_polices", "nb_sinistres",
    "loss_ratio", "delai_moyen_declaration",
]
CHURN_COLS = [
    "client_id", "segment", "churn_score", "churn_segment", "region_code",
]

TABLES = {
    "claims":           ("silver/claims",           CLAIMS_COLS),
    "fraud_assessment": ("gold/fraud_assessment",   FRAUD_COLS),
    "portfolio_kpis":   ("gold/portfolio_kpis",     KPIS_COLS),
    "churn_scores":     ("gold/churn_scores",       CHURN_COLS),
}

for name, (path, keep_cols) in TABLES.items():
    print(f"Export {path}…")
    df = read_delta(path)
    if df.empty:
        print(f"  WARN: table vide -- {path}")
        continue
    cols = [c for c in keep_cols if c in df.columns]
    df = df[cols]
    out = EXPORTS / f"{name}.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    kb = out.stat().st_size / 1024
    print(f"  OK {out.name}  {len(df):,} lignes  {kb:.0f} KB")

# Rapport HTML
report_src = BASE_DIR / "reports" / "quality_report.html"
if report_src.exists():
    shutil.copy(report_src, EXPORTS / "quality_report.html")
    kb = (EXPORTS / "quality_report.html").stat().st_size / 1024
    print(f"  OK quality_report.html  {kb:.0f} KB")
else:
    print("  WARN: reports/quality_report.html introuvable -- lance generate_report.py d'abord")

print("\nExport terminé. Fichiers dans exports/")
