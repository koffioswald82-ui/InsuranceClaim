# Architecture AXA Claims Lakehouse

## Vue d'ensemble

Le pipeline implémente une **architecture Medallion** (Bronze / Silver / Gold)
sur Azure Data Lake Storage Gen2, orchestrée par Azure Data Factory et
calculée par Azure Databricks (PySpark 3.5 + Delta Lake 3.x).

---

## Diagramme Mermaid — Pipeline complet

```mermaid
flowchart TD
    subgraph SOURCES["Sources hétérogènes"]
        S1[/"claims.csv\n(Système sinistres)"/]
        S2[/"policies.json\n(Système polices)"/]
        S3[/"clients.parquet\n(CRM)"/]
        S4[/"payments.csv\n(Comptabilité)"/]
        S5[/"INSEE / Banque de France\n(Données externes)"/]
    end

    subgraph INGESTION["Ingestion Layer"]
        I1["ingest_claims.py\nCast + Metadata"]
        I2["ingest_policies.py\nExplode garanties[]"]
        I3["schema_validator.py\nDead Letter Queue"]
    end

    subgraph BRONZE["🥉 Bronze — Raw + Metadata"]
        B1[("Delta Lake\nbronze/claims")]
        B2[("Delta Lake\nbronze/policies")]
        B3[("Delta Lake\nbronze/clients")]
        B4[("Delta Lake\nbronze/payments")]
        DLQ[("dead_letter_queue/\nRejets schéma")]
    end

    subgraph SILVER["🥈 Silver — Cleaned + Joined"]
        SV1[("Delta Lake\nsilver/claims\n+ enrichi")]
        SV2[("Delta Lake\nsilver/policies")]
        SV3[("Delta Lake\nsilver/clients")]
    end

    subgraph GOLD["🥇 Gold — Business Views"]
        G1[("portfolio_kpis\nLoss Ratio Q/YTD/12M")]
        G2[("fraud_assessment\nRules 001-005\nFraud Score 0-100")]
        G3[("churn_scores\nScore 0-100\npar client")]
        G4[("claims_aging\nBuckets 0-7j/7-30j\n30-90j/90j+")]
        G5[("portfolio_exposure\nExposition région\nHHI concentration")]
        G6[("earned_premium\ntable de réconciliation")]
    end

    subgraph MONITORING["Monitoring & Quality"]
        QR["data_quality_report.html"]
        PM["pipeline_metrics.json"]
        TESTS["pytest\nBronze/Silver/Gold"]
    end

    subgraph ORCHESTRATION["Orchestration"]
        ADF["Azure Data Factory\nadf_pipeline.json"]
        AIRFLOW["Apache Airflow\ndag_claims.py"]
    end

    S1 --> I1 --> I3
    S2 --> I2 --> I3
    S3 --> I3
    S4 --> I3
    S5 --> SV1

    I3 -->|"valides"| B1
    I3 -->|"valides"| B2
    I3 -->|"valides"| B3
    I3 -->|"valides"| B4
    I3 -->|"rejets"| DLQ

    B1 --> |"bronze_to_silver.py\nDédup + Clean + Join"| SV1
    B2 --> SV2
    B3 --> SV3

    SV1 --> |"silver_to_gold.py\nAggregation + EP"| G1
    SV1 --> |"fraud_flags.py\nRules Engine"| G2
    SV1 --> |"churn_score.py"| G3
    SV1 --> |"claims_aging.py"| G4
    SV2 --> |"portfolio_exposure.py"| G5
    SV2 --> G6

    G1 --> QR
    G2 --> QR
    G3 --> QR
    QR --> PM

    ADF -.->|"trigger"| I1
    AIRFLOW -.->|"orchestrate"| I1

    style BRONZE fill:#cd7f32,color:#fff
    style SILVER fill:#c0c0c0,color:#333
    style GOLD fill:#ffd700,color:#333
    style SOURCES fill:#e8f4fd,color:#333
    style MONITORING fill:#f0f4e8,color:#333
```

---

## Couches Medallion

| Couche | Format | Partitionnement | Rétention | Usage |
|--------|--------|-----------------|-----------|-------|
| **Bronze** | Delta Lake | année_ingestion / mois_ingestion | 7 ans (Solvency II) | Auditabilité, rejeu |
| **Silver** | Delta Lake | annee_sinistre / mois_sinistre | 5 ans | Analyse, ML |
| **Gold** | Delta Lake | annee / trimestre / produit | 3 ans rolling | Reporting, BI, API |

---

## Flux de données détaillé

### 1. Ingestion → Bronze
- Lecture multi-format (CSV, JSON nested, Parquet)
- Cast des types + normalisation strings
- Validation schéma stricte (`schema_validator.py`)
- Ajout métadonnées : `ingestion_timestamp`, `source_system`, `batch_id`
- Rejet dans `dead_letter_queue/` avec motif

### 2. Bronze → Silver (`bronze_to_silver.py`)
- Déduplication sur clé naturelle (row_number + ingestion_timestamp)
- Nettoyage : trim, lowercase, remplacement nulls métier
- Standardisation dates → `DateType` ISO 8601
- Jointure left (claims ← policies ← clients) avec flag `has_valid_policy`
- Calcul `delai_declaration_jours`
- Partitionnement par `annee_sinistre / mois_sinistre`

### 3. Silver → Gold (`silver_to_gold.py`)
- Calcul `earned_premium` au prorata temporis (période 2021-2024)
- Calcul `incurred_losses` = indemnisés (clos) + provisions (ouvert/en_cours)
- Agrégation 5D : produit × région × canal × année × trimestre
- Métriques window : YTD, rolling 12 mois
- Optimisation lecture : Z-order, auto-compact

### 4. Fraud Detection (`fraud_flags.py`)
Cinq règles métier avec score pondéré :

| Règle | Condition | Poids |
|-------|-----------|-------|
| RULE_001 | Sinistre < 48h après souscription | 25 pts |
| RULE_002 | Montant > mean + 3σ produit | 30 pts |
| RULE_003 | 3+ sinistres / client / 12 mois | 20 pts |
| RULE_004 | Même adresse, produits différents, < 30j | 15 pts |
| RULE_005 | Délai déclaration > 180 jours | 10 pts |

`fraud_score = Σ(poids × flag)` → borné [0, 100]

---

## Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Langage | Python | 3.11 |
| Framework distributed | PySpark | 3.5.1 |
| Format lakehouse | Delta Lake | 3.1.0 |
| Qualité données | Great Expectations | 0.18.15 |
| Orchestration | Apache Airflow | 2.9.1 |
| Orchestration cloud | Azure Data Factory | — |
| CI/CD | Azure DevOps Pipelines | — |
| Stockage | Azure Data Lake Gen2 (local: filesystem) | — |
| Monitoring | Jinja2 HTML Reports | 3.1.x |

---

## Décisions d'architecture

- [ADR-001 : Delta Lake vs Parquet vs Iceberg](ADR/ADR-001-delta-lake.md)
- [ADR-002 : Architecture Medallion vs Data Vault vs ODS](ADR/ADR-002-medallion.md)
