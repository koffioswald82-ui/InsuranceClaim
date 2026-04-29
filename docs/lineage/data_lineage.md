# Data Lineage — AXA Claims Lakehouse

> Traçabilité complète champ Gold → transformation → champ Silver → champ Bronze → source

---

## Table Gold : `portfolio_kpis`

| Champ Gold | Transformation | Champ Silver | Champ Bronze | Source |
|-----------|----------------|-------------|-------------|--------|
| `loss_ratio` | `incurred_losses / earned_premium × 100` | `montant_indemnise`, `montant_provision` | `montant_indemnise`, `montant_provision` | `claims.csv` |
| `earned_premium` | `prime_annuelle × (days_covered / 365)` | `prime_annuelle`, `date_souscription`, `date_echeance` | `prime_annuelle` | `policies.json` |
| `incurred_losses` | `SUM(montant_indemnise) WHERE clos + SUM(provision) WHERE ouvert` | `montant_indemnise`, `montant_provision`, `statut` | `montant_declare`, `statut` | `claims.csv` |
| `nb_sinistres` | `COUNT(claim_id)` | `claim_id` | `claim_id` | `claims.csv` |
| `nb_polices` | `COUNT DISTINCT(policy_id)` | `policy_id` | `policy_id` | `policies.json` |
| `delai_moyen_declaration` | `AVG(delai_declaration_jours)` | `delai_declaration_jours = datediff(date_declaration, date_sinistre)` | `date_declaration`, `date_sinistre` | `claims.csv` |
| `produit` | Passthrough + `lower()` | `produit` | `produit` | `policies.json` |
| `region_code` | Passthrough | `region_code` | `region_code` | `claims.csv` / `policies.json` |
| `canal_vente` | Passthrough + `lower()` | `canal_vente` | `canal_vente` | `policies.json` |
| `annee` | `YEAR(date_souscription)` | `annee_sinistre` | `date_sinistre` | `claims.csv` |
| `trimestre` | `CONCAT(YEAR, '-Q', QUARTER)` | `trimestre` | `date_sinistre` | `claims.csv` |
| `ytd_incurred_losses` | `SUM(incurred_losses) OVER(PARTITION BY produit, region, canal, annee ORDER BY trimestre ROWS UNBOUNDED PRECEDING)` | `incurred_losses` | — | Agrégat |
| `rolling_12m_loss_ratio` | `SUM(4 trimestres) / SUM(4 trimestres earned_premium) × 100` | `incurred_losses`, `earned_premium` | — | Agrégat |

---

## Table Gold : `fraud_assessment`

| Champ Gold | Transformation | Champ Silver | Champ Bronze | Source |
|-----------|----------------|-------------|-------------|--------|
| `flag_early_claim` | `CASE WHEN datediff(date_sinistre, date_souscription) < 2 THEN 1` | `date_sinistre`, `date_souscription` | `date_sinistre`, `date_souscription` | `claims.csv`, `policies.json` |
| `flag_high_amount` | `CASE WHEN montant_declare > mean(produit) + 3*stddev(produit)` | `montant_declare`, `produit` | `montant_declare` | `claims.csv` |
| `flag_frequent_claimant` | `COUNT(claim_id) OVER(PARTITION BY client_id, 365j WINDOW) >= 3` | `client_id`, `date_sinistre` | `client_id`, `date_sinistre` | `claims.csv` |
| `flag_address_cluster` | `Self-join ON adresse WHERE produit_L ≠ produit_R AND abs(days_diff) < 30` | `adresse`, `produit`, `date_sinistre` | `adresse` | `clients.parquet`, `claims.csv` |
| `flag_late_declaration` | `CASE WHEN delai_declaration_jours > 180` | `delai_declaration_jours` | `date_declaration`, `date_sinistre` | `claims.csv` |
| `fraud_score` | `Σ(flag_i × weight_i)` borné [0,100] | Tous les flags | — | Règles métier |
| `fraud_flags` | `ARRAY_COMPACT([IF(flag_001, 'RULE_001', NULL), ...])` | Tous les flags | — | Règles métier |
| `is_fraud_suspected` | `fraud_score > 0` | `fraud_score` | — | Dérivé |

---

## Table Gold : `churn_scores`

| Champ Gold | Transformation | Champ Silver | Champ Bronze | Source |
|-----------|----------------|-------------|-------------|--------|
| `score_seniority_inverse` | `1 - MIN(anciennete_annees, 10) / 10` | `date_creation_client` | `date_creation` | `clients.parquet` |
| `score_unpaid` | `nb_impayes / total_paiements` (borné [0,1]) | `statut` des paiements | `statut` | `payments.csv` |
| `score_refused` | `nb_refuses / total_sinistres` (borné [0,1]) | `statut = 'refuse'` | `statut` | `claims.csv` |
| `score_mono_contract` | `CASE WHEN COUNT(policy_id actif) ≤ 1 THEN 1 ELSE 0` | `policy_id`, `statut=actif` | `policy_id`, `statut` | `policies.json` |
| `churn_score` | `(0.3 × sen + 0.3 × unpaid + 0.2 × refused + 0.2 × mono) × 100` | Tous les scores composantes | — | Dérivé |
| `churn_segment` | `CASE WHEN churn_score < 25 → faible …` | `churn_score` | — | Dérivé |

---

## Table Gold : `claims_aging_summary`

| Champ Gold | Transformation | Champ Silver | Champ Bronze | Source |
|-----------|----------------|-------------|-------------|--------|
| `delai_traitement_jours` | `datediff(COALESCE(date_cloture, current_date), date_declaration)` | `date_cloture`, `date_declaration` | `date_cloture`, `date_declaration` | `claims.csv` |
| `aging_bucket` | `CASE WHEN ≤7 → '0_7j' WHEN ≤30 → '7_30j' …` | `delai_traitement_jours` | — | Dérivé |
| `delai_median_jours` | `PERCENTILE_APPROX(delai_traitement_jours, 0.5)` | `delai_traitement_jours` | — | Agrégat |
| `delai_p90_jours` | `PERCENTILE_APPROX(delai_traitement_jours, 0.9)` | `delai_traitement_jours` | — | Agrégat |

---

## Table Gold : `portfolio_exposure`

| Champ Gold | Transformation | Champ Silver | Champ Bronze | Source |
|-----------|----------------|-------------|-------------|--------|
| `exposition_max` | `SUM(plafond_garantie) WHERE statut=actif` | `plafond_garantie`, `statut` | `plafond_garantie` | `policies.json` |
| `prime_totale` | `SUM(prime_annuelle) WHERE statut=actif` | `prime_annuelle` | `prime_annuelle` | `policies.json` |
| `portfolio_hhi` | `Σ(share_produit²) × 10000` | `prime_annuelle`, `produit` | — | Dérivé (HHI) |
| `population` | Jointure données INSEE | `region_code` | `region_code` | `insee_regions.csv` |
| `sinistres_per_policy` | `nb_sinistres / nb_polices` | `claim_id`, `policy_id` | — | Dérivé |

---

## Traçabilité RGPD (Article 30 RGPD)

| Donnée personnelle | Table source | Tables Gold | Pseudonymisation | Chiffrement |
|--------------------|-------------|-------------|-----------------|-------------|
| Nom / Prénom | `silver/clients` | Non exposé en Gold | HMAC-SHA256 | AES-256 ADLS |
| Email | `silver/clients` | Non exposé en Gold | Oui (Gold) | AES-256 ADLS |
| Adresse | `silver/clients` | Non exposé en Gold | Oui (Gold) | AES-256 ADLS |
| Date de naissance | `silver/clients` | Non exposé en Gold | Oui | AES-256 ADLS |
| `client_id` (UUID) | Toutes tables Silver | Toutes tables Gold | Non (UUID opaque) | Non requis |
