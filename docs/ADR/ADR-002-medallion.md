# ADR-002 — Architecture des données : Medallion vs Data Vault vs ODS

**Statut** : Accepté  
**Date** : 2024-01-20  
**Décideurs** : Data Architect, Chief Data Officer, Responsable Conformité

---

## Contexte

L'équipe Data Engineering doit concevoir l'architecture de stockage et
d'organisation des données pour le pipeline sinistres AXA France, supportant :

1. **Analytique temps réel** : loss ratio à J+1
2. **Machine Learning** : détection de fraude, churn
3. **Conformité réglementaire** : Solvency II, RGPD, data lineage
4. **Accès multi-équipes** : actuaires, risk managers, data scientists, BI

Trois patterns ont été évalués : **Medallion (Lakehouse)**, **Data Vault 2.0**,
**ODS + EDW classique**.

---

## Décision

**L'architecture Medallion** (Bronze / Silver / Gold) est retenue.

---

## Analyse comparative

### ODS + EDW classique (Star Schema)

```
Sources → ETL → ODS (staging) → DWH (étoile/flocon) → Cubes OLAP → Rapports
```

**Avantages :**
- Modèle éprouvé, bien compris des actuaires
- Performance requêtes analytiques excellente

**Inconvénients :**
- ETL long et fragile : toute modification source = refonte
- Pas adapté aux données non structurées / semi-structurées
- Impossible de rejouer le pipeline sur les données brutes
- Pas de support natif ML (features engineering complexe)
- Conformité RGPD difficile (pas de lineage granulaire)

**Verdict** : Inadapté à l'ère lakehouse et aux volumes semi-structurés.

---

### Data Vault 2.0

```
Sources → Staging → Raw Vault (Hubs + Links + Satellites) → Business Vault → Information Mart
```

**Avantages :**
- Auditabilité native (chaque satellite horodaté)
- Très flexible face aux évolutions sources
- Excellent pour l'historisation

**Inconvénients :**
- Complexité de modélisation très élevée (30+ tables pour sinistres)
- Courbe d'apprentissage abrupte (6-12 mois)
- Performances analytiques médiocres sans Business Vault bien conçu
- Outillage spécialisé (Wherescape, dbt Vault) nécessaire
- Inadapté aux pipelines ML natifs

**Verdict** : Sur-dimensionné pour notre usage, ROI trop long.

---

### Medallion Architecture (Lakehouse)

```
Sources → Bronze (raw) → Silver (clean + joined) → Gold (business views)
```

**Avantages :**
- **Auditabilité** : Bronze = copie fidèle des sources (immuable), Delta time travel
- **Flexibilité** : ajout d'une nouvelle couche Gold sans impacter Silver
- **ML natif** : Silver = feature store naturel pour scikit-learn / MLflow
- **Coût** : pas de duplication source→staging→DWH
- **Conformité** : lineage Bronze→Silver→Gold traçable par colonne
- **Temps réel** : Delta Streaming unifie batch et streaming
- **Montée en charge** : scalabilité linéaire avec Spark

**Inconvénients :**
- Moins familier pour les équipes BI habituées aux cubes OLAP
- Nécessite un catalogue de données (Unity Catalog)
- Gestion de la qualité plus explicite (pas de contraintes DWH)

---

## Conséquences

### Définition des couches

| Couche | Principe | Immutabilité | Qui y accède |
|--------|----------|-------------|--------------|
| **Bronze** | Données brutes + métadonnées ingestion | Quasi-immuable | Data Engineers |
| **Silver** | Données nettoyées, jointes, enrichies | Versionné (MERGE) | Data Scientists, Ingénieurs |
| **Gold** | Vues métier agrégées, optimisées lecture | Reconstruit périodiquement | Actuaires, BI, API |

### Règles d'or
1. **Bronze ne se supprime jamais** — tout est rejoué depuis Bronze
2. **Silver ne contient pas de logique métier** — nettoyage technique uniquement
3. **Gold ne contient pas de données brutes** — uniquement des agrégats ou scores
4. **Chaque transformation est idempotente** — `mode("overwrite")` sur Silver/Gold

### Gouvernance
- **Unity Catalog** : catalogage Bronze/Silver/Gold, column-level security
- **Data lineage** : tracé via Azure Purview ou OpenLineage
- **Data Quality** : Great Expectations sur toutes les couches

---

## Alternatives non retenues

- **Lambda Architecture** : duplication batch/streaming complexe, remplacé par le Medallion
- **Kappa Architecture** : tout-streaming inadapté aux gros volumes historiques assurance
