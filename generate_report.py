"""Génère un rapport qualité HTML à partir des tables Gold/Silver du lakehouse."""
import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pathlib import Path
from datetime import datetime

from src.utils.config import GOLD_PATH, SILVER_PATH
from src.utils.spark_session import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session()

# ── Lecture des tables ─────────────────────────────────────────────────────────
print("Chargement des tables...")
claims    = spark.read.format("delta").load(f"{SILVER_PATH}/claims")
policies  = spark.read.format("delta").load(f"{SILVER_PATH}/policies")
clients   = spark.read.format("delta").load(f"{SILVER_PATH}/clients")
fraud     = spark.read.format("delta").load(f"{GOLD_PATH}/fraud_assessment")
kpis      = spark.read.format("delta").load(f"{GOLD_PATH}/portfolio_kpis")
churn     = spark.read.format("delta").load(f"{GOLD_PATH}/churn_scores")

# ── KPIs globaux ───────────────────────────────────────────────────────────────
print("Calcul des KPIs...")

n_claims   = claims.count()
n_policies = policies.dropDuplicates(["policy_id"]).count()
n_clients  = clients.count()
n_fraud    = fraud.count()
n_suspected = fraud.filter(F.col("is_fraud_suspected")).count()
fraud_rate  = round(n_suspected / n_fraud * 100, 2) if n_fraud > 0 else 0

# Loss ratio moyen
avg_loss_ratio = kpis.select(F.mean("loss_ratio")).collect()[0][0]
avg_loss_ratio = round(avg_loss_ratio * 100, 2) if avg_loss_ratio else 0

# Montant moyen déclaré
avg_montant = claims.select(F.mean("montant_declare")).collect()[0][0]
avg_montant = round(avg_montant, 2) if avg_montant else 0

# Délai déclaration moyen
avg_delai = claims.select(F.mean("delai_declaration_jours")).collect()[0][0]
avg_delai = round(avg_delai, 1) if avg_delai else 0

# Taux nulls colonnes critiques
null_rates = {}
for col in ["claim_id", "policy_id", "client_id", "date_sinistre", "montant_declare"]:
    n_null = claims.filter(F.col(col).isNull()).count()
    null_rates[col] = round(n_null / n_claims * 100, 3) if n_claims > 0 else 0

# Distribution statut sinistres
statut_dist = (
    claims.groupBy("statut").count()
    .orderBy("count", ascending=False)
    .collect()
)

# Distribution produits
produit_dist = (
    claims.groupBy("produit").count()
    .orderBy("count", ascending=False)
    .collect()
)

# Churn par segment
churn_dist = (
    churn.groupBy("churn_segment").count()
    .orderBy("count", ascending=False)
    .collect()
)

# Loss ratio par produit (top 5)
lr_by_produit = (
    kpis.groupBy("produit")
    .agg(F.mean("loss_ratio").alias("lr"))
    .orderBy("lr", ascending=False)
    .limit(5)
    .collect()
)

# Fraud score distribution
fraud_bins = (
    fraud
    .withColumn("score_bin",
        F.when(F.col("fraud_score") == 0,   "0 (aucun)")
        .when(F.col("fraud_score") <= 25,  "1-25 (faible)")
        .when(F.col("fraud_score") <= 50,  "26-50 (moyen)")
        .when(F.col("fraud_score") <= 75,  "51-75 (élevé)")
        .otherwise("76-100 (critique)")
    )
    .groupBy("score_bin").count()
    .orderBy("score_bin")
    .collect()
)

spark.stop()

# ── Génération HTML ────────────────────────────────────────────────────────────
print("Génération du rapport HTML...")

def pct_bar(value, max_val=100, color="#3b82f6"):
    pct = min(value / max_val * 100, 100) if max_val > 0 else 0
    return f'<div style="background:#e5e7eb;border-radius:4px;height:12px;width:100%"><div style="background:{color};width:{pct:.1f}%;height:12px;border-radius:4px"></div></div>'

def kpi_card(label, value, sub="", color="#3b82f6"):
    return f'''
    <div style="background:white;border-radius:12px;padding:20px 24px;box-shadow:0 1px 4px rgba(0,0,0,.08);min-width:160px;flex:1">
      <div style="font-size:13px;color:#6b7280;margin-bottom:6px">{label}</div>
      <div style="font-size:28px;font-weight:700;color:{color}">{value}</div>
      <div style="font-size:12px;color:#9ca3af;margin-top:4px">{sub}</div>
    </div>'''

now = datetime.now().strftime("%d/%m/%Y %H:%M")

html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AXA Claims Lakehouse - Rapport Qualité</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f3f4f6;margin:0;padding:24px;color:#111827}}
  h1{{font-size:24px;font-weight:700;margin:0 0 4px}}
  h2{{font-size:16px;font-weight:600;margin:20px 0 12px;color:#374151;border-bottom:1px solid #e5e7eb;padding-bottom:6px}}
  .meta{{font-size:13px;color:#6b7280;margin-bottom:24px}}
  .grid{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}}
  table{{width:100%;border-collapse:collapse;background:white;border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
  th{{background:#f9fafb;font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;padding:10px 16px;text-align:left}}
  td{{padding:10px 16px;font-size:14px;border-top:1px solid #f3f4f6}}
  tr:hover td{{background:#f9fafb}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:99px;font-size:12px;font-weight:600}}
  .ok{{background:#dcfce7;color:#166534}}
  .warn{{background:#fef9c3;color:#854d0e}}
  .err{{background:#fee2e2;color:#991b1b}}
  .section{{background:white;border-radius:12px;padding:20px 24px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:20px}}
</style>
</head>
<body>
<h1>AXA Claims Lakehouse - Rapport Qualité</h1>
<div class="meta">Généré le {now} · Pipeline PySpark 3.5.1 + Delta Lake 3.1.0</div>

<h2>KPIs Pipeline</h2>
<div class="grid">
  {kpi_card("Sinistres Silver", f"{n_claims:,}", "après déduplication")}
  {kpi_card("Polices uniques", f"{n_policies:,}", "Silver policies")}
  {kpi_card("Clients", f"{n_clients:,}", "Silver clients")}
  {kpi_card("Taux de fraude", f"{fraud_rate}%", f"{n_suspected:,} suspects", "#ef4444")}
  {kpi_card("Loss Ratio moyen", f"{avg_loss_ratio}%", "Gold KPIs", "#f59e0b")}
  {kpi_card("Montant moyen", f"{avg_montant:,.0f} €", "montant_declare")}
  {kpi_card("Délai déclaration", f"{avg_delai} j", "moyenne", "#8b5cf6")}
</div>

<h2>Qualité des données - Taux de nulls (colonnes critiques)</h2>
<div class="section">
<table>
<tr><th>Colonne</th><th>Taux de nulls</th><th>Statut</th><th>Visualisation</th></tr>
"""
for col, rate in null_rates.items():
    status = '<span class="badge ok">OK</span>' if rate < 2 else '<span class="badge err">KO</span>'
    html += f'<tr><td><code>{col}</code></td><td>{rate}%</td><td>{status}</td><td style="width:200px">{pct_bar(rate, 5, "#ef4444" if rate >= 2 else "#22c55e")}</td></tr>\n'

html += f"""</table>
</div>

<h2>Distribution des statuts sinistres</h2>
<div class="section">
<table>
<tr><th>Statut</th><th>Nombre</th><th>% du total</th><th>Répartition</th></tr>
"""
max_statut = max((r["count"] for r in statut_dist), default=1)
for r in statut_dist:
    pct = round(r["count"] / n_claims * 100, 1)
    html += f'<tr><td>{r["statut"]}</td><td>{r["count"]:,}</td><td>{pct}%</td><td style="width:200px">{pct_bar(r["count"], max_statut)}</td></tr>\n'

html += f"""</table>
</div>

<h2>Répartition par produit</h2>
<div class="section">
<table>
<tr><th>Produit</th><th>Sinistres</th><th>Loss Ratio moyen</th><th>Volume</th></tr>
"""
lr_map = {r["produit"]: round(r["lr"]*100, 1) for r in lr_by_produit}
max_prod = max((r["count"] for r in produit_dist), default=1)
for r in produit_dist:
    lr = lr_map.get(r["produit"], "-")
    lr_str = f'{lr}%' if lr != "-" else "-"
    lr_color = "#ef4444" if isinstance(lr, float) and lr > 80 else "#22c55e"
    badge = f'<span class="badge" style="background:#dbeafe;color:#1e40af">{lr_str}</span>' if lr != "-" else "-"
    html += f'<tr><td>{r["produit"]}</td><td>{r["count"]:,}</td><td>{badge}</td><td style="width:200px">{pct_bar(r["count"], max_prod, "#3b82f6")}</td></tr>\n'

html += f"""</table>
</div>

<h2>Détection de fraude - Distribution des scores</h2>
<div class="section">
<table>
<tr><th>Tranche de score</th><th>Sinistres</th><th>%</th><th>Volume</th></tr>
"""
colors = ["#22c55e","#84cc16","#f59e0b","#ef4444","#7c3aed"]
max_bin = max((r["count"] for r in fraud_bins), default=1)
for i, r in enumerate(fraud_bins):
    pct = round(r["count"] / n_fraud * 100, 1) if n_fraud > 0 else 0
    html += f'<tr><td>{r["score_bin"]}</td><td>{r["count"]:,}</td><td>{pct}%</td><td style="width:200px">{pct_bar(r["count"], max_bin, colors[i % len(colors)])}</td></tr>\n'

html += f"""</table>
</div>

<h2>Score de churn - Segmentation clients</h2>
<div class="section">
<table>
<tr><th>Segment</th><th>Clients</th><th>%</th><th>Volume</th></tr>
"""
seg_colors = {"critique":"#7c3aed","élevé":"#ef4444","moyen":"#f59e0b","faible":"#22c55e"}
max_seg = max((r["count"] for r in churn_dist), default=1)
for r in churn_dist:
    pct = round(r["count"] / n_clients * 100, 1) if n_clients > 0 else 0
    seg = r.asDict().get("churn_segment") or "?"
    color = seg_colors.get(seg, "#6b7280")
    html += f'<tr><td><span class="badge" style="background:{color}22;color:{color}">{seg}</span></td><td>{r["count"]:,}</td><td>{pct}%</td><td style="width:200px">{pct_bar(r["count"], max_seg, color)}</td></tr>\n'

html += f"""</table>
</div>

<div style="margin-top:32px;font-size:12px;color:#9ca3af;text-align:center">
  AXA Claims Lakehouse · PySpark 3.5.1 · Delta Lake 3.1.0 · Généré le {now}
</div>
</body>
</html>"""

out_path = Path("reports") / "quality_report.html"
out_path.parent.mkdir(exist_ok=True)
out_path.write_text(html, encoding="utf-8")
print(f"Rapport généré : {out_path.resolve()}")
