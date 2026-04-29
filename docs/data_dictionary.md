# Data Dictionary — AXA Claims Lakehouse

> **Convention** : `sensibilité_RGPD` = 🔴 Données personnelles identifiantes |
> 🟠 Données sensibles indirectes | 🟢 Non personnelles

---

## Table : `silver/claims` (sinistres)

| Champ | Type | Description | Nullable | Sensibilité RGPD | SLA Fraîcheur |
|-------|------|-------------|----------|------------------|---------------|
| `claim_id` | STRING | Identifiant unique du sinistre (UUID) | NON | 🟢 | J+1 |
| `policy_id` | STRING | Clé étrangère vers la police | NON | 🟢 | J+1 |
| `client_id` | STRING | Clé étrangère vers le client | NON | 🟠 | J+1 |
| `type_sinistre` | STRING | Type : incendie, vol, accident_auto, etc. | NON | 🟢 | J+1 |
| `date_sinistre` | DATE | Date de survenance du sinistre | OUI | 🟢 | J+1 |
| `date_declaration` | DATE | Date de déclaration auprès d'AXA | OUI | 🟢 | J+1 |
| `date_cloture` | DATE | Date de clôture du dossier | OUI | 🟢 | J+3 |
| `montant_declare` | DOUBLE | Montant déclaré par l'assuré (€) | OUI | 🟢 | J+1 |
| `montant_provision` | DOUBLE | Provision comptable constituée (€) | OUI | 🟢 | J+1 |
| `montant_indemnise` | DOUBLE | Montant effectivement indemnisé (€) | OUI | 🟢 | J+3 |
| `statut` | STRING | ouvert / en_cours / clos / refuse / en_litige | NON | 🟢 | J+1 |
| `region_code` | STRING | Code INSEE région (ex. "11" = Île-de-France) | OUI | 🟢 | J+1 |
| `description` | STRING | Description libre du sinistre | OUI | 🔴 Anonymiser avant export | J+1 |
| `produit` | STRING | Produit de la police associée | OUI | 🟢 | J+1 |
| `prime_annuelle` | DOUBLE | Prime annuelle de la police associée (€) | OUI | 🟢 | J+1 |
| `canal_vente` | STRING | Canal de vente de la police | OUI | 🟢 | J+1 |
| `delai_declaration_jours` | INT | date_declaration - date_sinistre (jours) | OUI | 🟢 | J+1 |
| `has_valid_policy` | BOOLEAN | La police est bien référencée | NON | 🟢 | J+1 |
| `has_valid_client` | BOOLEAN | Le client est bien référencé | NON | 🟢 | J+1 |
| `annee_sinistre` | STRING | Année de survenance (partition) | NON | 🟢 | J+1 |
| `mois_sinistre` | STRING | Mois de survenance (partition, 2 chiffres) | NON | 🟢 | J+1 |
| `trimestre` | STRING | Format : YYYY-QN (ex. 2023-Q2) | OUI | 🟢 | J+1 |
| `silver_processed_at` | TIMESTAMP | Horodatage transformation Silver | NON | 🟢 | J+1 |

---

## Table : `silver/policies` (polices)

| Champ | Type | Description | Nullable | Sensibilité RGPD | SLA Fraîcheur |
|-------|------|-------------|----------|------------------|---------------|
| `policy_id` | STRING | Identifiant unique de la police (UUID) | NON | 🟢 | J+1 |
| `client_id` | STRING | Clé étrangère client | NON | 🟠 | J+1 |
| `produit` | STRING | auto / habitation / sante / vie / mrh | NON | 🟢 | J+1 |
| `date_souscription` | DATE | Date de prise d'effet | OUI | 🟢 | J+1 |
| `date_echeance` | DATE | Date d'échéance contractuelle | OUI | 🟢 | J+1 |
| `prime_annuelle` | DOUBLE | Prime annuelle brute TTC (€) | OUI | 🟢 | J+1 |
| `prime_mensuelle` | DOUBLE | Prime mensuelle (prime_annuelle / 12) | OUI | 🟢 | J+1 |
| `canal_vente` | STRING | agent / courtier / digital / direct / partenaire | OUI | 🟢 | J+1 |
| `statut` | STRING | actif / résilié / suspendu | NON | 🟢 | J+1 |
| `region_code` | STRING | Code INSEE région | OUI | 🟢 | J+1 |
| `franchise` | DOUBLE | Franchise contractuelle (€) | OUI | 🟢 | J+1 |
| `plafond_garantie` | DOUBLE | Plafond de garantie (€) | OUI | 🟢 | J+1 |
| `garantie_code` | STRING | Code de la garantie (ligne par garantie) | OUI | 🟢 | J+1 |
| `garantie_libelle` | STRING | Libellé de la garantie | OUI | 🟢 | J+1 |
| `garantie_plafond` | DOUBLE | Plafond de la garantie (€) | OUI | 🟢 | J+1 |
| `garantie_incluse` | BOOLEAN | Garantie incluse dans la formule | OUI | 🟢 | J+1 |

---

## Table : `silver/clients` (clients)

| Champ | Type | Description | Nullable | Sensibilité RGPD | SLA Fraîcheur |
|-------|------|-------------|----------|------------------|---------------|
| `client_id` | STRING | Identifiant unique client (UUID) | NON | 🟠 | J+1 |
| `client_type` | STRING | particulier / professionnel / entreprise | NON | 🟢 | J+7 |
| `nom` | STRING | Nom de famille ou raison sociale | OUI | 🔴 Chiffrer au repos | J+7 |
| `prenom` | STRING | Prénom (particuliers uniquement) | OUI | 🔴 Chiffrer au repos | J+7 |
| `email` | STRING | Adresse email | OUI | 🔴 Pseudonymiser | J+7 |
| `telephone` | STRING | Numéro de téléphone | OUI | 🔴 Pseudonymiser | J+7 |
| `adresse` | STRING | Adresse postale | OUI | 🔴 Chiffrer au repos | J+7 |
| `code_postal` | STRING | Code postal français | OUI | 🟠 | J+7 |
| `ville` | STRING | Ville | OUI | 🟠 | J+7 |
| `region_code` | STRING | Code INSEE région | OUI | 🟢 | J+7 |
| `date_creation` | DATE | Date de création du compte client | OUI | 🟢 | J+7 |
| `canal_acquisition` | STRING | Canal d'acquisition | OUI | 🟢 | J+7 |
| `segment` | STRING | silver / gold / platinum | OUI | 🟢 | J+7 |
| `actif` | BOOLEAN | Client actif dans le CRM | NON | 🟢 | J+1 |
| `date_naissance` | DATE | Date de naissance (particuliers) | OUI | 🔴 Chiffrer au repos | J+30 |

---

## Table : `gold/fraud_assessment` (évaluation fraude)

| Champ | Type | Description | Nullable | Sensibilité RGPD | SLA Fraîcheur |
|-------|------|-------------|----------|------------------|---------------|
| `claim_id` | STRING | Clé du sinistre évalué | NON | 🟢 | J+1 |
| `policy_id` | STRING | Clé de la police | NON | 🟢 | J+1 |
| `client_id` | STRING | Clé du client | NON | 🟠 | J+1 |
| `produit` | STRING | Produit concerné | OUI | 🟢 | J+1 |
| `date_sinistre` | DATE | Date de survenance | OUI | 🟢 | J+1 |
| `montant_declare` | DOUBLE | Montant déclaré (€) | OUI | 🟢 | J+1 |
| `flag_early_claim` | INT | 1 si sinistre < 48h après souscription | NON | 🟢 | J+1 |
| `flag_high_amount` | INT | 1 si montant > mean + 3σ | NON | 🟢 | J+1 |
| `flag_frequent_claimant` | INT | 1 si 3+ sinistres / 12 mois | NON | 🟢 | J+1 |
| `flag_address_cluster` | INT | 1 si cluster adresse multi-produits | NON | 🟢 | J+1 |
| `flag_late_declaration` | INT | 1 si délai > 180j | NON | 🟢 | J+1 |
| `fraud_score` | INT | Score pondéré [0, 100] | NON | 🟢 | J+1 |
| `fraud_flags` | ARRAY<STRING> | Liste des règles activées | NON | 🟢 | J+1 |
| `is_fraud_suspected` | BOOLEAN | fraud_score > 0 | NON | 🟢 | J+1 |
| `fraud_assessment_at` | TIMESTAMP | Horodatage du calcul | NON | 🟢 | J+1 |

---

## Table : `gold/churn_scores` (score churn client)

| Champ | Type | Description | Nullable | Sensibilité RGPD | SLA Fraîcheur |
|-------|------|-------------|----------|------------------|---------------|
| `client_id` | STRING | Clé client | NON | 🟠 | J+1 |
| `client_type` | STRING | Type de client | OUI | 🟢 | J+7 |
| `nom` | STRING | Nom (pseudonymisé en Gold) | OUI | 🔴 | J+7 |
| `region_code` | STRING | Région INSEE | OUI | 🟢 | J+7 |
| `segment` | STRING | Segment commercial | OUI | 🟢 | J+7 |
| `anciennete_annees` | DOUBLE | Ancienneté en années | OUI | 🟢 | J+1 |
| `nb_contrats` | INT | Nombre de polices actives | OUI | 🟢 | J+1 |
| `nb_impayes` | INT | Nombre de paiements impayés | OUI | 🟢 | J+1 |
| `nb_refuses` | INT | Nombre de sinistres refusés | OUI | 🟢 | J+1 |
| `score_seniority_inverse` | DOUBLE | Composante ancienneté [0,1] | NON | 🟢 | J+1 |
| `score_unpaid` | DOUBLE | Composante impayés [0,1] | NON | 🟢 | J+1 |
| `score_refused` | DOUBLE | Composante refus [0,1] | NON | 🟢 | J+1 |
| `score_mono_contract` | DOUBLE | Composante mono-contrat [0,1] | NON | 🟢 | J+1 |
| `churn_score` | DOUBLE | Score final [0, 100] | NON | 🟢 | J+1 |
| `churn_segment` | STRING | faible / moyen / élevé / critique | NON | 🟢 | J+1 |
| `churn_computed_at` | TIMESTAMP | Horodatage du calcul | NON | 🟢 | J+1 |

---

## Mapping RGPD — Mesures techniques

| Catégorie | Champs concernés | Mesure |
|-----------|-----------------|--------|
| Identification directe | nom, prénom, email, téléphone, adresse | Chiffrement AES-256 au repos (ADLS CMK) |
| Identification indirecte | client_id, date_naissance | Pseudonymisation par hashage HMAC-SHA256 |
| Données médicales | type_sinistre=hospitalisation/dentaire/optique | Accès restreint (column-level security) |
| Données financières | montants, primes, paiements | Masquage dans les environnements non-prod |

**Durée de rétention (RGPD Art. 5) :**
- Données personnelles actives : durée du contrat + 5 ans
- Données archivées Solvency II : 10 ans minimum
- Logs d'accès : 1 an
