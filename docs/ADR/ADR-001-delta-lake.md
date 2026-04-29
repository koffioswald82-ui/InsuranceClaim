# ADR-001 — Choix du format de stockage : Delta Lake

**Statut** : Accepté  
**Date** : 2024-01-15  
**Décideurs** : Data Engineering Lead, Architecte Cloud, DSI

---

## Contexte

Le pipeline AXA Claims Lakehouse nécessite un format de fichier pour stocker
120 000+ sinistres, 80 000+ polices et 50 000+ clients avec :

- Mises à jour et corrections fréquentes (rectificatifs de sinistres)
- Lectures analytiques intensives (loss ratio, fraude)
- Conformité auditabilité Solvency II (qui a modifié quoi, quand)
- Évolution de schéma sans downtime (nouveaux champs réglementaires)
- Rollback en cas de pipeline défectueux

Trois formats ont été évalués : **Parquet**, **Delta Lake**, **Apache Iceberg**.

---

## Décision

**Delta Lake v3.x est retenu** comme format unique pour les trois couches
Bronze, Silver et Gold.

---

## Analyse comparative

### Parquet (format de base)

| Critère | Parquet |
|---------|---------|
| ACID | ❌ Pas de transactions |
| Time Travel | ❌ Impossible nativement |
| Schema Evolution | ⚠️ Limité (ajout colonnes seulement) |
| DML (UPDATE/DELETE) | ❌ Réécriture complète de partitions |
| Streaming + Batch | ⚠️ Complexe à unifier |
| Écosystème Azure | ✅ Natif |
| Performance lecture | ✅ Excellent |

**Verdict** : Insuffisant pour nos contraintes ACID et auditabilité.

### Apache Iceberg

| Critère | Iceberg |
|---------|---------|
| ACID | ✅ Transactions complètes |
| Time Travel | ✅ Snapshots |
| Schema Evolution | ✅ Complet |
| DML | ✅ Row-level UPDATE/DELETE |
| Streaming + Batch | ✅ |
| Écosystème Azure | ⚠️ Nécessite Spark + catalogue externe |
| Support Databricks | ⚠️ Secondaire (Delta natif) |
| Maturité opérationnelle AXA | ❌ Compétence absente |

**Verdict** : Techniquement équivalent mais coût d'adoption et
manque de support Databricks natif sont rédhibitoires.

### Delta Lake 3.x

| Critère | Delta Lake |
|---------|-----------|
| ACID | ✅ Transactions sérialisables |
| Time Travel | ✅ `VERSION AS OF n` / `TIMESTAMP AS OF` |
| Schema Evolution | ✅ `mergeSchema`, `overwriteSchema` |
| DML | ✅ `MERGE INTO`, `UPDATE`, `DELETE` |
| Streaming + Batch | ✅ Unifié via Delta streaming |
| Écosystème Azure Databricks | ✅ Natif (format par défaut) |
| Optimisation auto | ✅ Auto-optimize, Z-order |
| Liquid Clustering | ✅ Delta 3.x |
| Compétence équipe | ✅ Déjà maîtrisé |

**Verdict** : Remporte tous les critères décisifs.

---

## Conséquences

### Positives
- **ACID** : les corrections de sinistres (MERGE INTO) sont atomiques
- **Time Travel** : audit Solvency II avec `DESCRIBE HISTORY`
- **Schema evolution** : ajout de colonnes RGPD sans migration
- **Rollback** : `RESTORE TABLE ... TO VERSION AS OF N` en cas de bug pipeline
- **Unity Catalog** : gouvernance centralisée sur Databricks

### Négatives / Risques
- Vendor lock-in relatif sur Databricks (atténué par le format ouvert)
- Overhead `_delta_log/` (~2 % d'espace disque supplémentaire)
- `VACUUM` régulier nécessaire pour purger les anciennes versions

### Décisions dérivées
- `VACUUM RETAIN 720 HOURS` (30 jours) en prod pour conformité
- Z-order sur colonnes `produit, region_code, date_sinistre` pour les tables Silver
- `AUTO OPTIMIZE` activé sur toutes les tables Gold

---

## Alternatives non retenues

- **Hudi** : écosystème AWS-centric, moins bien intégré à Azure Databricks
- **ORC** : format Hive legacy, pas d'ACID hors Hive
