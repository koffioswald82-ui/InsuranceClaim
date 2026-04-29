"""
Générateur de rapport HTML de qualité des données.

Produit un rapport autonome avec Jinja2 :
taux de nulls, doublons, distributions, anomalies, évolution KPIs qualité.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.config import BRONZE_PATH, GOLD_PATH, REPORTS_DIR, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

log = get_logger("monitoring.data_quality_report")

# ── Template HTML ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AXA Claims Lakehouse — Data Quality Report</title>
<style>
  body { font-family: 'Segoe UI', sans-serif; background: #f5f7fa; color: #333; margin: 0; }
  .header { background: linear-gradient(135deg, #003d82, #0066cc); color: white;
            padding: 30px 40px; }
  .header h1 { margin: 0; font-size: 28px; }
  .header p  { margin: 5px 0 0; opacity: .8; }
  .container { max-width: 1200px; margin: 30px auto; padding: 0 20px; }
  .kpi-grid  { display: grid; grid-template-columns: repeat(4,1fr); gap: 20px; margin: 20px 0; }
  .kpi-card  { background: white; border-radius: 10px; padding: 20px; text-align: center;
               box-shadow: 0 2px 8px rgba(0,0,0,.08); }
  .kpi-value { font-size: 36px; font-weight: bold; color: #003d82; }
  .kpi-label { color: #666; font-size: 13px; margin-top: 6px; }
  .section   { background: white; border-radius: 10px; padding: 25px;
               margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,.08); }
  .section h2 { color: #003d82; margin-top: 0; border-bottom: 2px solid #e8edf3;
                padding-bottom: 10px; }
  table  { width: 100%; border-collapse: collapse; }
  th     { background: #003d82; color: white; padding: 10px 14px; text-align: left; }
  td     { padding: 9px 14px; border-bottom: 1px solid #e8edf3; }
  tr:hover td { background: #f0f4ff; }
  .badge-ok    { background: #d4edda; color: #155724; padding: 3px 10px;
                 border-radius: 12px; font-size: 12px; }
  .badge-warn  { background: #fff3cd; color: #856404; padding: 3px 10px;
                 border-radius: 12px; font-size: 12px; }
  .badge-error { background: #f8d7da; color: #721c24; padding: 3px 10px;
                 border-radius: 12px; font-size: 12px; }
  .progress    { background: #e9ecef; border-radius: 4px; height: 8px; }
  .progress-bar { background: #003d82; height: 8px; border-radius: 4px; }
  footer { text-align: center; color: #888; padding: 30px;
           font-size: 12px; border-top: 1px solid #e8edf3; margin-top: 40px; }
</style>
</head>
<body>
<div class="header">
  <h1>AXA Claims Lakehouse — Data Quality Report</h1>
  <p>Généré le {{ report_date }} | Batch {{ batch_id }}</p>
</div>

<div class="container">

  <!-- KPIs globaux -->
  <div class="kpi-grid">
    {% for kpi in global_kpis %}
    <div class="kpi-card">
      <div class="kpi-value">{{ kpi.value }}</div>
      <div class="kpi-label">{{ kpi.label }}</div>
    </div>
    {% endfor %}
  </div>

  <!-- Taux de nulls par table -->
  <div class="section">
    <h2>Taux de nulls par couche et colonne critique</h2>
    <table>
      <tr><th>Couche</th><th>Table</th><th>Colonne</th><th>Taux nuls</th><th>Statut</th></tr>
      {% for row in null_rates %}
      <tr>
        <td>{{ row.layer }}</td>
        <td>{{ row.table }}</td>
        <td>{{ row.column }}</td>
        <td>
          <div class="progress">
            <div class="progress-bar" style="width:{{ [row.null_rate_pct, 100]|min }}%"></div>
          </div>
          {{ "%.2f"|format(row.null_rate_pct) }} %
        </td>
        <td>
          {% if row.null_rate_pct < 1 %}
            <span class="badge-ok">OK</span>
          {% elif row.null_rate_pct < 5 %}
            <span class="badge-warn">WARN</span>
          {% else %}
            <span class="badge-error">CRITICAL</span>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <!-- Doublons -->
  <div class="section">
    <h2>Contrôle des doublons</h2>
    <table>
      <tr><th>Table</th><th>Lignes totales</th><th>Lignes distinctes</th>
          <th>Doublons</th><th>Statut</th></tr>
      {% for row in duplicate_checks %}
      <tr>
        <td>{{ row.table }}</td>
        <td>{{ "{:,}".format(row.total) }}</td>
        <td>{{ "{:,}".format(row.distinct) }}</td>
        <td>{{ "{:,}".format(row.duplicates) }}</td>
        <td>
          {% if row.duplicates == 0 %}
            <span class="badge-ok">OK</span>
          {% else %}
            <span class="badge-error">DOUBLONS DÉTECTÉS</span>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <!-- KPIs fraude -->
  <div class="section">
    <h2>Fraude — Distribution des scores</h2>
    <table>
      <tr><th>Tranche score</th><th>Nb sinistres</th><th>% total</th></tr>
      {% for row in fraud_dist %}
      <tr>
        <td>{{ row.bucket }}</td>
        <td>{{ "{:,}".format(row.count) }}</td>
        <td>{{ "%.2f"|format(row.pct) }} %</td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <!-- Anomalies temporelles -->
  <div class="section">
    <h2>Anomalies détectées</h2>
    <table>
      <tr><th>Type</th><th>Description</th><th>Nb lignes</th><th>Sévérité</th></tr>
      {% for a in anomalies %}
      <tr>
        <td>{{ a.type }}</td>
        <td>{{ a.description }}</td>
        <td>{{ "{:,}".format(a.count) }}</td>
        <td>
          {% if a.severity == "low" %}
            <span class="badge-ok">LOW</span>
          {% elif a.severity == "medium" %}
            <span class="badge-warn">MEDIUM</span>
          {% else %}
            <span class="badge-error">HIGH</span>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </table>
  </div>

</div>
<footer>
  AXA Claims Lakehouse | Pipeline Data Engineering | {{ report_date }}
</footer>
</body>
</html>
"""


# ── Collecte des métriques ─────────────────────────────────────────────────────

def _collect_null_rates(spark) -> list[dict[str, Any]]:
    """Collecte les taux de nulls sur les colonnes critiques."""
    from pyspark.sql import functions as F

    checks = [
        (BRONZE_PATH, "bronze", "claims",   ["claim_id", "policy_id", "client_id", "montant_declare"]),
        (SILVER_PATH, "silver", "claims",   ["claim_id", "policy_id", "date_sinistre"]),
        (GOLD_PATH,   "gold",   "fraud_assessment", ["claim_id", "fraud_score"]),
    ]

    results = []
    for base, layer, table, cols in checks:
        try:
            df = spark.read.format("delta").load(f"{base}/{table}")
            n = df.count()
            for col in cols:
                if col in df.columns:
                    n_null = df.filter(F.col(col).isNull()).count()
                    rate = (n_null / n * 100) if n > 0 else 0.0
                    results.append({
                        "layer": layer, "table": table,
                        "column": col, "null_rate_pct": round(rate, 4),
                    })
        except Exception as e:
            log.warning(f"Cannot read {layer}/{table}", error=str(e))

    return results


def _collect_duplicate_checks(spark) -> list[dict[str, Any]]:
    """Vérifie les doublons sur les clés naturelles."""
    checks = [
        (BRONZE_PATH, "bronze/claims",   "claim_id"),
        (SILVER_PATH, "silver/claims",   "claim_id"),
        (SILVER_PATH, "silver/clients",  "client_id"),
        (GOLD_PATH,   "gold/fraud_assessment", "claim_id"),
        (GOLD_PATH,   "gold/churn_scores",     "client_id"),
    ]

    results = []
    for base, path, key_col in checks:
        try:
            df = spark.read.format("delta").load(f"{base}/{path.split('/')[-1]}")
            n_total    = df.count()
            n_distinct = df.select(key_col).distinct().count()
            results.append({
                "table":      path,
                "total":      n_total,
                "distinct":   n_distinct,
                "duplicates": n_total - n_distinct,
            })
        except Exception as e:
            log.warning(f"Cannot check {path}", error=str(e))

    return results


def _collect_fraud_distribution(spark) -> list[dict[str, Any]]:
    """Distribution du fraud_score en buckets."""
    from pyspark.sql import functions as F
    try:
        df = spark.read.format("delta").load(f"{GOLD_PATH}/fraud_assessment")
        n = df.count()
        if n == 0:
            return []

        buckets = [
            ("0",       df.filter(F.col("fraud_score") == 0).count()),
            ("1-25",    df.filter((F.col("fraud_score") > 0)  & (F.col("fraud_score") <= 25)).count()),
            ("26-50",   df.filter((F.col("fraud_score") > 25) & (F.col("fraud_score") <= 50)).count()),
            ("51-75",   df.filter((F.col("fraud_score") > 50) & (F.col("fraud_score") <= 75)).count()),
            ("76-100",  df.filter(F.col("fraud_score") > 75).count()),
        ]
        return [{"bucket": b, "count": c, "pct": c / n * 100} for b, c in buckets]
    except Exception as e:
        log.warning("Cannot read fraud_assessment", error=str(e))
        return []


def _detect_anomalies(spark) -> list[dict[str, Any]]:
    """Détecte les anomalies connues."""
    from pyspark.sql import functions as F
    anomalies = []
    try:
        claims = spark.read.format("delta").load(f"{SILVER_PATH}/claims")

        # Délais négatifs
        n = claims.filter(
            F.col("delai_declaration_jours").isNotNull() &
            (F.col("delai_declaration_jours") < 0)
        ).count()
        if n > 0:
            anomalies.append({
                "type": "Délai négatif",
                "description": "date_declaration < date_sinistre",
                "count": n, "severity": "high",
            })

        # Montants extrêmes
        n2 = claims.filter(F.col("montant_declare") > 1_000_000).count()
        if n2 > 0:
            anomalies.append({
                "type": "Montant extrême",
                "description": "montant_declare > 1 000 000 €",
                "count": n2, "severity": "medium",
            })

        # Sinistres sans produit
        n3 = claims.filter(F.col("produit").isNull()).count()
        if n3 > 0:
            anomalies.append({
                "type": "Sinistre orphelin",
                "description": "Aucun produit associé (police manquante)",
                "count": n3, "severity": "medium",
            })
    except Exception as e:
        log.warning("Cannot analyze anomalies", error=str(e))

    return anomalies or [{"type": "Aucune", "description": "Aucune anomalie détectée",
                          "count": 0, "severity": "low"}]


# ── Génération ─────────────────────────────────────────────────────────────────

def generate_report(output_path: str | None = None) -> Path:
    """Génère le rapport HTML de qualité des données.

    Args:
        output_path: Chemin du fichier HTML de sortie.

    Returns:
        Path vers le fichier généré.
    """
    try:
        from jinja2 import Template
    except ImportError:
        log.error("jinja2 not installed — pip install jinja2")
        raise

    _output = Path(output_path or str(REPORTS_DIR / "data_quality_report.html"))

    log.info("Generating data quality report", output=str(_output))

    spark = get_spark_session(app_name="data_quality_report")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d")

    null_rates       = _collect_null_rates(spark)
    duplicate_checks = _collect_duplicate_checks(spark)
    fraud_dist       = _collect_fraud_distribution(spark)
    anomalies        = _detect_anomalies(spark)

    # KPIs globaux
    try:
        n_claims  = spark.read.format("delta").load(f"{SILVER_PATH}/claims").count()
        n_clients = spark.read.format("delta").load(f"{SILVER_PATH}/clients").count()
        n_fraud   = spark.read.format("delta").load(f"{GOLD_PATH}/fraud_assessment").filter(
            "is_fraud_suspected = true"
        ).count()
        n_anomalies = sum(a["count"] for a in anomalies)
    except Exception:
        n_claims = n_clients = n_fraud = n_anomalies = 0

    global_kpis = [
        {"label": "Sinistres Silver",     "value": f"{n_claims:,}"},
        {"label": "Clients",              "value": f"{n_clients:,}"},
        {"label": "Fraudes suspectées",   "value": f"{n_fraud:,}"},
        {"label": "Anomalies détectées",  "value": f"{n_anomalies:,}"},
    ]

    template = Template(HTML_TEMPLATE)
    html = template.render(
        report_date=datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC"),
        batch_id=batch_id,
        global_kpis=global_kpis,
        null_rates=null_rates,
        duplicate_checks=duplicate_checks,
        fraud_dist=fraud_dist,
        anomalies=anomalies,
    )

    _output.write_text(html, encoding="utf-8")
    log.info("Report generated", path=str(_output), size_kb=len(html) // 1024)
    return _output


if __name__ == "__main__":
    path = generate_report()
    print(f"Rapport généré : {path}")
