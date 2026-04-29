"""
Générateur de données synthétiques réalistes pour AXA Claims Lakehouse.

Produit :
  - 50 000 clients (particuliers, pros, entreprises)
  - 80 000 polices (auto, habitation, santé, vie, MRH)
  - 120 000 sinistres sur 3 ans
  - 200 000 paiements (~4 % d'impayés)
  - ~3 % de patterns frauduleux injectés
  - Données externes INSEE + inflation Banque de France
"""

from __future__ import annotations

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker
from faker.providers import address, company, person, phone_number

# ── Configuration ──────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("fr_FR")
fake.seed_instance(SEED)
fake.add_provider(address)
fake.add_provider(company)
fake.add_provider(person)
fake.add_provider(phone_number)

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
EXT_DIR = BASE_DIR / "data" / "external"
RAW_DIR.mkdir(parents=True, exist_ok=True)
EXT_DIR.mkdir(parents=True, exist_ok=True)

N_CLIENTS = 50_000
N_POLICIES = 80_000
N_CLAIMS = 120_000
N_PAYMENTS = 200_000

FRAUD_RATE = 0.03

START_DATE = date(2021, 1, 1)
END_DATE = date(2024, 12, 31)

# ── INSEE regions ──────────────────────────────────────────────────────────────
INSEE_REGIONS: list[dict[str, Any]] = [
    {"code": "11", "nom": "Île-de-France",        "population": 12_278_210, "densite": 1014},
    {"code": "24", "nom": "Centre-Val de Loire",  "population": 2_573_849,  "densite":  65},
    {"code": "27", "nom": "Bourgogne-Franche-Comté","population":2_807_100, "densite":  59},
    {"code": "28", "nom": "Normandie",             "population": 3_303_500,  "densite": 114},
    {"code": "32", "nom": "Hauts-de-France",       "population": 5_962_662,  "densite": 189},
    {"code": "44", "nom": "Grand Est",             "population": 5_511_747,  "densite": 100},
    {"code": "52", "nom": "Pays de la Loire",      "population": 3_801_797,  "densite": 114},
    {"code": "53", "nom": "Bretagne",              "population": 3_395_346,  "densite": 125},
    {"code": "75", "nom": "Nouvelle-Aquitaine",    "population": 6_059_256,  "densite":  70},
    {"code": "76", "nom": "Occitanie",             "population": 6_066_743,  "densite":  77},
    {"code": "84", "nom": "Auvergne-Rhône-Alpes",  "population": 8_059_950,  "densite": 120},
    {"code": "93", "nom": "Provence-Alpes-Côte d'Azur","population":5_098_321,"densite":156},
    {"code": "94", "nom": "Corse",                 "population":  345_449,   "densite":  39},
]

REGION_CODES = [r["code"] for r in INSEE_REGIONS]
REGION_WEIGHTS = [r["population"] for r in INSEE_REGIONS]
TOTAL_POP = sum(REGION_WEIGHTS)
REGION_PROBS = [w / TOTAL_POP for w in REGION_WEIGHTS]

# ── Product definitions ────────────────────────────────────────────────────────
PRODUCTS = ["auto", "habitation", "sante", "vie", "mrh"]
PRODUCT_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]

ANNUAL_PREMIUM_RANGES: dict[str, tuple[float, float]] = {
    "auto":       (400,  2_500),
    "habitation": (200,  1_200),
    "sante":      (800,  3_600),
    "vie":        (500,  5_000),
    "mrh":        (250,  1_500),
}

CLAIM_TYPE_BY_PRODUCT: dict[str, list[str]] = {
    "auto":       ["accident_auto", "vol", "bris_glace", "catastrophe_naturelle"],
    "habitation": ["degat_eau", "incendie", "vol", "catastrophe_naturelle"],
    "sante":      ["hospitalisation", "optique", "dentaire", "pharmacie"],
    "vie":        ["deces", "invalidite"],
    "mrh":        ["degat_eau", "incendie", "vol", "bris_glace"],
}

CLAIM_AMOUNT_RANGES: dict[str, tuple[float, float]] = {
    "incendie":            (5_000,  80_000),
    "vol":                 (500,    15_000),
    "degat_eau":           (1_000,  20_000),
    "accident_auto":       (2_000,  50_000),
    "bris_glace":          (200,     2_500),
    "catastrophe_naturelle":(3_000, 60_000),
    "hospitalisation":     (500,    20_000),
    "optique":             (100,     1_500),
    "dentaire":            (200,     5_000),
    "pharmacie":           (20,        500),
    "deces":               (50_000,500_000),
    "invalidite":          (20_000,200_000),
}

CHANNELS = ["agent", "courtier", "digital", "direct", "partenaire"]
CLIENT_TYPES = ["particulier", "professionnel", "entreprise"]
CLIENT_TYPE_WEIGHTS = [0.70, 0.20, 0.10]

CLAIM_STATUS = ["ouvert", "en_cours", "clos", "refuse", "en_litige"]
CLAIM_STATUS_WEIGHTS = [0.15, 0.20, 0.45, 0.12, 0.08]

PAYMENT_STATUS = ["paye", "impaye", "en_cours", "rembourse"]
PAYMENT_STATUS_WEIGHTS = [0.82, 0.04, 0.10, 0.04]


# ── Helpers ────────────────────────────────────────────────────────────────────

def rand_date(start: date, end: date) -> date:
    """Renvoie une date aléatoire entre start et end inclus."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def rand_datetime(start: date, end: date) -> datetime:
    """Renvoie un datetime aléatoire entre start et end."""
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


def weighted_choice(population: list, weights: list) -> Any:
    """Sélection pondérée."""
    return random.choices(population, weights=weights, k=1)[0]


def region_code() -> str:
    return np.random.choice(REGION_CODES, p=REGION_PROBS)


# ── 1. Clients ─────────────────────────────────────────────────────────────────

def generate_clients(n: int = N_CLIENTS) -> pd.DataFrame:
    """Génère n clients avec attributs réalistes."""
    print(f"[generate] Clients : {n:,}")
    rows = []
    for _ in range(n):
        ctype = weighted_choice(CLIENT_TYPES, CLIENT_TYPE_WEIGHTS)
        if ctype == "particulier":
            nom = fake.last_name()
            prenom = fake.first_name()
        elif ctype == "professionnel":
            nom = fake.last_name()
            prenom = fake.first_name()
        else:
            nom = fake.company()
            prenom = ""

        birth_year = random.randint(1940, 2000) if ctype != "entreprise" else None
        rows.append({
            "client_id":       str(uuid.uuid4()),
            "client_type":     ctype,
            "nom":             nom,
            "prenom":          prenom,
            "date_naissance":  date(birth_year, random.randint(1, 12), random.randint(1, 28)).isoformat()
                               if birth_year else None,
            "email":           fake.email(),
            "telephone":       fake.phone_number(),
            "adresse":         fake.street_address(),
            "code_postal":     fake.postcode(),
            "ville":           fake.city(),
            "region_code":     region_code(),
            "date_creation":   rand_date(date(2015, 1, 1), END_DATE).isoformat(),
            "canal_acquisition": weighted_choice(CHANNELS, [0.25, 0.20, 0.30, 0.15, 0.10]),
            "segment":         random.choice(["silver", "gold", "platinum"]),
            "actif":           random.choices([True, False], weights=[0.92, 0.08])[0],
        })
    return pd.DataFrame(rows)


# ── 2. Polices ─────────────────────────────────────────────────────────────────

def generate_policies(clients_df: pd.DataFrame, n: int = N_POLICIES) -> list[dict]:
    """Génère n polices associées aux clients."""
    print(f"[generate] Polices : {n:,}")
    client_ids = clients_df["client_id"].tolist()
    policies = []
    for _ in range(n):
        product = weighted_choice(PRODUCTS, PRODUCT_WEIGHTS)
        pmin, pmax = ANNUAL_PREMIUM_RANGES[product]
        prime = round(random.uniform(pmin, pmax), 2)
        start = rand_date(date(2018, 1, 1), date(2024, 6, 30))
        duration_months = random.choice([12, 24, 36, 60])
        end = start + timedelta(days=duration_months * 30)

        garanties = _build_garanties(product, prime)

        policies.append({
            "policy_id":       str(uuid.uuid4()),
            "client_id":       random.choice(client_ids),
            "produit":         product,
            "date_souscription": start.isoformat(),
            "date_echeance":   end.isoformat(),
            "prime_annuelle":  prime,
            "prime_mensuelle": round(prime / 12, 2),
            "canal_vente":     weighted_choice(CHANNELS, [0.25, 0.20, 0.30, 0.15, 0.10]),
            "statut":          random.choices(["actif", "resilié", "suspendu"], weights=[0.80, 0.15, 0.05])[0],
            "region_code":     region_code(),
            "franchise":       round(random.choice([0, 150, 300, 500, 1000]), 2),
            "plafond_garantie": round(random.uniform(prime * 10, prime * 50), 2),
            "garanties":       garanties,
        })
    return policies


def _build_garanties(product: str, prime: float) -> list[dict]:
    """Construit la liste des garanties pour un produit donné."""
    base: dict[str, list[str]] = {
        "auto":       ["RC", "dommages", "vol", "bris_glace", "assistance"],
        "habitation": ["incendie", "degat_eau", "vol", "RC", "catastrophe_naturelle"],
        "sante":      ["hospitalisation", "optique", "dentaire", "pharmacie"],
        "vie":        ["deces", "invalidite", "epargne"],
        "mrh":        ["incendie", "degat_eau", "vol", "bris_glace", "RC"],
    }
    garanties = []
    for g in base.get(product, []):
        garanties.append({
            "code":    g,
            "libelle": g.replace("_", " ").title(),
            "plafond": round(random.uniform(prime * 5, prime * 30), 2),
            "incluse": random.choices([True, False], weights=[0.85, 0.15])[0],
        })
    return garanties


# ── 3. Sinistres ───────────────────────────────────────────────────────────────

def generate_claims(policies: list[dict], n: int = N_CLAIMS) -> pd.DataFrame:
    """Génère n sinistres avec ~3 % de patterns frauduleux injectés."""
    print(f"[generate] Sinistres : {n:,}  (fraude ~{FRAUD_RATE*100:.0f} %)")
    policy_map = {p["policy_id"]: p for p in policies}
    policy_ids = list(policy_map.keys())

    rows = []
    fraud_targets = set(random.sample(range(n), int(n * FRAUD_RATE)))

    client_claim_counter: dict[str, list[date]] = {}

    for i in range(n):
        pid = random.choice(policy_ids)
        pol = policy_map[pid]
        product = pol["produit"]
        claim_types = CLAIM_TYPE_BY_PRODUCT[product]
        ctype = random.choice(claim_types)
        amt_min, amt_max = CLAIM_AMOUNT_RANGES[ctype]

        pol_start = date.fromisoformat(pol["date_souscription"])
        pol_end = date.fromisoformat(pol["date_echeance"])
        claim_date_min = max(pol_start, START_DATE)
        claim_date_max = min(pol_end, END_DATE)
        if claim_date_min >= claim_date_max:
            claim_date_max = claim_date_min + timedelta(days=365)

        sinistre_date = rand_date(claim_date_min, claim_date_max)

        is_fraud = i in fraud_targets
        fraud_rule = None

        if is_fraud:
            fraud_pattern = random.choice(["early_claim", "high_amount", "frequent_claimant"])
            if fraud_pattern == "early_claim":
                sinistre_date = pol_start + timedelta(hours=random.randint(1, 47))
                sinistre_date = sinistre_date.date() if hasattr(sinistre_date, "date") else sinistre_date
                fraud_rule = "early_claim"
            elif fraud_pattern == "high_amount":
                amt_max = amt_max * 4
                fraud_rule = "high_amount"
            else:
                fraud_rule = "frequent_claimant"

        # déclaration : 0-30j après sinistre (occasionnellement tardive)
        decl_delay = random.choices(
            [random.randint(0, 7), random.randint(8, 30), random.randint(31, 90), random.randint(91, 300)],
            weights=[0.40, 0.35, 0.15, 0.10],
        )[0]
        declaration_date = sinistre_date + timedelta(days=decl_delay)

        montant = round(random.uniform(amt_min, amt_max), 2)
        provision = round(montant * random.uniform(0.5, 1.0), 2)
        indemnise = round(montant * random.uniform(0.6, 0.95), 2) if random.random() > 0.15 else 0.0

        client_id = pol["client_id"]
        if client_id not in client_claim_counter:
            client_claim_counter[client_id] = []
        client_claim_counter[client_id].append(sinistre_date)

        rows.append({
            "claim_id":          str(uuid.uuid4()),
            "policy_id":         pid,
            "client_id":         client_id,
            "type_sinistre":     ctype,
            "date_sinistre":     sinistre_date.isoformat(),
            "date_declaration":  declaration_date.isoformat(),
            "date_cloture":      (declaration_date + timedelta(days=random.randint(1, 180))).isoformat()
                                 if random.random() > 0.35 else None,
            "montant_declare":   montant,
            "montant_provision": provision,
            "montant_indemnise": indemnise,
            "statut":            weighted_choice(CLAIM_STATUS, CLAIM_STATUS_WEIGHTS),
            "region_code":       pol["region_code"],
            "description":       fake.sentence(nb_words=12),
            "is_fraud_injected": is_fraud,
            "fraud_rule_injected": fraud_rule,
        })

    df = pd.DataFrame(rows)

    # Injecter frequent_claimant : forcer 3+ sinistres sur 12 mois pour des clients cibles
    _inject_frequent_claimant(df, n_targets=int(n * FRAUD_RATE * 0.33))

    return df


def _inject_frequent_claimant(df: pd.DataFrame, n_targets: int) -> None:
    """Inject frequent claimant fraud patterns in-place."""
    client_ids = df["client_id"].unique().tolist()
    targets = random.sample(client_ids, min(n_targets, len(client_ids)))
    anchor_dates = {c: rand_date(date(2022, 1, 1), date(2024, 6, 1)) for c in targets}
    mask = df["client_id"].isin(targets)
    for i in df[mask].index[:n_targets]:
        c = df.at[i, "client_id"]
        anchor = anchor_dates[c]
        offset = random.randint(0, 11)
        df.at[i, "date_sinistre"] = (anchor + timedelta(days=offset * 30)).isoformat()
        df.at[i, "date_declaration"] = (anchor + timedelta(days=offset * 30 + 2)).isoformat()
        df.at[i, "is_fraud_injected"] = True
        df.at[i, "fraud_rule_injected"] = "frequent_claimant"


# ── 4. Paiements ───────────────────────────────────────────────────────────────

def generate_payments(claims_df: pd.DataFrame, n: int = N_PAYMENTS) -> pd.DataFrame:
    """Génère n paiements liés aux sinistres (~4 % impayés)."""
    print(f"[generate] Paiements : {n:,}")
    claim_ids = claims_df["claim_id"].tolist()
    # Index O(1) pour éviter la recherche linéaire sur 120k lignes
    claims_index = claims_df.set_index("claim_id")[
        ["date_declaration", "policy_id", "client_id", "montant_declare"]
    ].to_dict("index")
    rows = []
    for _ in range(n):
        cid = random.choice(claim_ids)
        claim_row = claims_index[cid]
        decl_date = date.fromisoformat(claim_row["date_declaration"])
        payment_date = decl_date + timedelta(days=random.randint(1, 90))
        montant = round(random.uniform(100, float(claim_row["montant_declare"] or 1000)), 2)
        rows.append({
            "payment_id":       str(uuid.uuid4()),
            "claim_id":         cid,
            "policy_id":        claim_row["policy_id"],
            "client_id":        claim_row["client_id"],  # type: ignore[index]
            "date_paiement":    payment_date.isoformat(),
            "montant":          montant,
            "devise":           "EUR",
            "statut":           weighted_choice(PAYMENT_STATUS, PAYMENT_STATUS_WEIGHTS),
            "mode_paiement":    random.choice(["virement", "cheque", "prelevement", "carte"]),
            "reference_banque": fake.bban(),
            "motif_impaye":     random.choice(["provision_insuffisante", "litige", "fraude_suspectee", None])
                                if random.random() < 0.04 else None,
        })
    return pd.DataFrame(rows)


# ── 5. Données externes ────────────────────────────────────────────────────────

def generate_external_data() -> None:
    """Génère les fichiers de données externes INSEE + inflation."""
    print("[generate] Données externes")

    insee_df = pd.DataFrame(INSEE_REGIONS)
    insee_df.to_csv(EXT_DIR / "insee_regions.csv", index=False)

    inflation = []
    rates = {2021: 1.6, 2022: 5.2, 2023: 4.9, 2024: 2.3}
    for year, rate in rates.items():
        for month in range(1, 13):
            monthly = round(rate / 12 + random.gauss(0, 0.05), 3)
            inflation.append({
                "annee": year,
                "mois":  month,
                "taux_annuel_pct": rate,
                "taux_mensuel_pct": monthly,
                "source": "Banque de France",
            })
    pd.DataFrame(inflation).to_csv(EXT_DIR / "inflation_rates.csv", index=False)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    """Point d'entrée principal : génère tous les fichiers de données brutes."""
    print("=" * 60)
    print("AXA Claims Lakehouse — Génération de données synthétiques")
    print("=" * 60)

    clients_df = generate_clients(N_CLIENTS)
    clients_df.to_parquet(RAW_DIR / "clients.parquet", index=False)
    print(f"  → clients.parquet ({len(clients_df):,} lignes)")

    policies = generate_policies(clients_df, N_POLICIES)
    with open(RAW_DIR / "policies.json", "w", encoding="utf-8") as f:
        json.dump(policies, f, ensure_ascii=False, default=str)
    print(f"  → policies.json ({len(policies):,} objets)")

    claims_df = generate_claims(policies, N_CLAIMS)
    claims_df.to_csv(RAW_DIR / "claims.csv", index=False)
    print(f"  → claims.csv ({len(claims_df):,} lignes, "
          f"{claims_df['is_fraud_injected'].sum()} fraudes injectées)")

    payments_df = generate_payments(claims_df, N_PAYMENTS)
    payments_df.to_csv(RAW_DIR / "payments.csv", index=False)
    impayes = (payments_df["statut"] == "impaye").sum()
    print(f"  → payments.csv ({len(payments_df):,} lignes, "
          f"{impayes} impayés = {impayes/len(payments_df)*100:.1f} %)")

    generate_external_data()
    print("  → insee_regions.csv + inflation_rates.csv")

    print("=" * 60)
    print("Génération terminée.")


if __name__ == "__main__":
    main()
