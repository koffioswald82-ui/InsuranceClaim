"""
Transformation Bronze → Silver.

Déduplication, nettoyage, standardisation dates, jointure des entités,
calcul du délai de déclaration, partitionnement et écriture Delta Lake.
"""

from __future__ import annotations

import uuid
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType, DoubleType

from src.utils.config import BRONZE_PATH, SILVER_PATH
from src.utils.logger import pipeline_step
from src.utils.spark_session import get_spark_session


# ── Lecture Bronze ─────────────────────────────────────────────────────────────

def read_bronze(
    spark: SparkSession,
    entity: str,
    bronze_path: str = BRONZE_PATH,
) -> DataFrame:
    """Lit une table Bronze Delta Lake.

    Args:
        spark: SparkSession active.
        entity: Nom de la table ('claims', 'policies', 'clients', 'payments').
        bronze_path: Chemin racine Bronze.

    Returns:
        DataFrame Bronze.
    """
    return spark.read.format("delta").load(f"{bronze_path}/{entity}")


# ── Déduplication ──────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame, natural_key: list[str]) -> DataFrame:
    """Déduplique sur clé naturelle en gardant la ligne la plus récente.

    Args:
        df: DataFrame potentiellement dupliqué.
        natural_key: Colonnes formant la clé naturelle.

    Returns:
        DataFrame sans doublons.
    """
    w = Window.partitionBy(*natural_key).orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ── Nettoyage ──────────────────────────────────────────────────────────────────

def clean_claims(df: DataFrame) -> DataFrame:
    """Nettoyage des sinistres : nulls métier, standardisation strings.

    Args:
        df: DataFrame claims Bronze.

    Returns:
        DataFrame nettoyé.
    """
    return (
        df
        .withColumn("montant_declare",
                    F.coalesce(F.col("montant_declare"), F.lit(0.0)))
        .withColumn("montant_provision",
                    F.coalesce(F.col("montant_provision"), F.lit(0.0)))
        .withColumn("montant_indemnise",
                    F.coalesce(F.col("montant_indemnise"), F.lit(0.0)))
        .withColumn("description",
                    F.coalesce(F.trim(F.col("description")), F.lit("")))
        .withColumn("statut",
                    F.coalesce(F.trim(F.lower(F.col("statut"))), F.lit("inconnu")))
        .withColumn("type_sinistre",
                    F.coalesce(F.trim(F.lower(F.col("type_sinistre"))), F.lit("inconnu")))
    )


def clean_policies(df: DataFrame) -> DataFrame:
    """Nettoyage des polices.

    Args:
        df: DataFrame policies Bronze.

    Returns:
        DataFrame nettoyé.
    """
    return (
        df
        .withColumn("prime_annuelle",
                    F.coalesce(F.col("prime_annuelle"), F.lit(0.0)))
        .withColumn("prime_mensuelle",
                    F.when(F.col("prime_mensuelle").isNull(),
                           F.col("prime_annuelle") / 12)
                    .otherwise(F.col("prime_mensuelle")))
        .withColumn("franchise",
                    F.coalesce(F.col("franchise"), F.lit(0.0)))
        .withColumn("statut",
                    F.coalesce(F.trim(F.lower(F.col("statut"))), F.lit("inconnu")))
        .withColumn("produit",
                    F.coalesce(F.trim(F.lower(F.col("produit"))), F.lit("inconnu")))
    )


# ── Standardisation dates → ISO 8601 ──────────────────────────────────────────

_DATE_FORMATS = [
    "yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy",
    "yyyy/MM/dd", "dd-MM-yyyy",
]


def _parse_date_col(df: DataFrame, col_name: str) -> DataFrame:
    """Parse une colonne date en essayant plusieurs formats."""
    parsed = None
    for fmt in _DATE_FORMATS:
        candidate = F.to_date(F.col(col_name), fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
    return df.withColumn(col_name, parsed)


def standardize_dates(df: DataFrame, date_cols: list[str]) -> DataFrame:
    """Standardise les colonnes dates en DateType ISO 8601.

    Args:
        df: DataFrame source.
        date_cols: Liste des colonnes à parser.

    Returns:
        DataFrame avec colonnes dates castées.
    """
    for col_name in date_cols:
        if col_name in df.columns:
            df = _parse_date_col(df, col_name)
    return df


# ── Délai de déclaration ───────────────────────────────────────────────────────

def compute_declaration_delay(df: DataFrame) -> DataFrame:
    """Calcule le délai de déclaration en jours.

    delai_declaration_jours = date_declaration - date_sinistre

    Les valeurs négatives sont nullifiées (données incohérentes).

    Args:
        df: DataFrame claims Silver.

    Returns:
        DataFrame avec colonne delai_declaration_jours.
    """
    df = df.withColumn(
        "delai_declaration_jours",
        F.datediff(F.col("date_declaration"), F.col("date_sinistre"))
    )
    # Annuler les valeurs négatives (incohérence source)
    df = df.withColumn(
        "delai_declaration_jours",
        F.when(F.col("delai_declaration_jours") < 0, F.lit(None))
        .otherwise(F.col("delai_declaration_jours"))
    )
    return df


# ── Jointures ──────────────────────────────────────────────────────────────────

def join_entities(
    claims_df: DataFrame,
    policies_df: DataFrame,
    clients_df: DataFrame,
) -> DataFrame:
    """Jointure sinistres × polices × clients avec gestion des orphelins.

    Les sinistres sans police (orphelins) sont conservés avec left join.
    Les sinistres sans client sont également conservés.

    Args:
        claims_df: DataFrame claims Silver.
        policies_df: DataFrame policies Silver (dédupliqué sur policy_id).
        clients_df: DataFrame clients Silver (dédupliqué sur client_id).

    Returns:
        DataFrame enrichi.
    """
    # Déduplique les polices sur policy_id (une ligne par police)
    pol_dedup = (
        policies_df
        .dropDuplicates(["policy_id"])
        .select(
            "policy_id", "produit", "prime_annuelle", "prime_mensuelle",
            "date_souscription", "date_echeance", "canal_vente",
            "franchise", "plafond_garantie",
            F.col("statut").alias("statut_police"),
        )
    )

    cli_dedup = (
        clients_df
        .dropDuplicates(["client_id"])
        .select(
            "client_id", "client_type", "nom",
            "canal_acquisition", "segment",
            F.col("date_creation").alias("date_creation_client"),
            F.col("actif").alias("client_actif"),
        )
    )

    joined = (
        claims_df
        .join(pol_dedup, on="policy_id", how="left")
        .join(cli_dedup, on="client_id", how="left")
    )

    # Flag orphelins
    joined = joined.withColumn(
        "has_valid_policy",
        F.col("produit").isNotNull()
    ).withColumn(
        "has_valid_client",
        F.col("client_type").isNotNull()
    )

    return joined


# ── Enrichissements Silver ─────────────────────────────────────────────────────

def add_silver_columns(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes de partitionnement et d'enrichissement Silver.

    Args:
        df: DataFrame joint.

    Returns:
        DataFrame enrichi avec colonnes Silver.
    """
    return (
        df
        .withColumn("annee_sinistre", F.year(F.col("date_sinistre")).cast("string"))
        .withColumn("mois_sinistre",  F.lpad(F.month(F.col("date_sinistre")).cast("string"), 2, "0"))
        .withColumn("trimestre",      F.concat(
            F.year(F.col("date_sinistre")).cast("string"),
            F.lit("-Q"),
            F.quarter(F.col("date_sinistre")).cast("string")
        ))
        .withColumn("silver_processed_at", F.current_timestamp())
    )


# ── Écriture Silver ────────────────────────────────────────────────────────────

def write_silver(
    df: DataFrame,
    entity: str,
    silver_path: str = SILVER_PATH,
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
) -> int:
    """Écrit un DataFrame en couche Silver Delta Lake.

    Args:
        df: DataFrame à écrire.
        entity: Nom de la table cible.
        silver_path: Chemin Silver.
        mode: Mode d'écriture.
        partition_cols: Colonnes de partitionnement.

    Returns:
        Nombre de lignes écrites.
    """
    target = f"{silver_path}/{entity}"
    w = (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.save(target)
    return -1


# ── Pipeline principal ─────────────────────────────────────────────────────────

def run_bronze_to_silver(
    spark: Optional[SparkSession] = None,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
    batch_id: Optional[str] = None,
) -> dict[str, int]:
    """Exécute la transformation complète Bronze → Silver.

    Args:
        spark: SparkSession (créée si None).
        bronze_path: Chemin Bronze source.
        silver_path: Chemin Silver cible.
        batch_id: Identifiant batch.

    Returns:
        Dictionnaire avec volumétrie par table.
    """
    _spark = spark or get_spark_session()
    _batch_id = batch_id or str(uuid.uuid4())[:8]
    results: dict[str, int] = {}

    with pipeline_step("bronze_to_silver", batch_id=_batch_id) as log:

        # ── Claims ─────────────────────────────────────────────────────────────
        log.info("Processing claims")
        claims_raw = read_bronze(_spark, "claims", bronze_path)

        claims = deduplicate(claims_raw, ["claim_id"])
        claims = clean_claims(claims)
        claims = standardize_dates(claims, ["date_sinistre", "date_declaration", "date_cloture"])
        claims = compute_declaration_delay(claims)
        claims = add_silver_columns(claims)
        claims = claims.localCheckpoint()  # materialise + coupe la lignée DAG

        # ── Policies ───────────────────────────────────────────────────────────
        log.info("Processing policies")
        policies_raw = read_bronze(_spark, "policies", bronze_path)
        policies = deduplicate(policies_raw, ["policy_id", "garantie_code"])
        policies = clean_policies(policies)
        policies = standardize_dates(policies, ["date_souscription", "date_echeance"])
        policies = policies.localCheckpoint()

        # ── Clients ────────────────────────────────────────────────────────────
        log.info("Processing clients")
        clients_raw = read_bronze(_spark, "clients", bronze_path)
        clients = deduplicate(clients_raw, ["client_id"])
        clients = clients.localCheckpoint()

        # ── Join ───────────────────────────────────────────────────────────────
        log.info("Joining entities")
        enriched_claims = join_entities(claims, policies, clients)

        # ── Write ──────────────────────────────────────────────────────────────
        log.info("Writing Silver tables")
        results["claims"]   = write_silver(enriched_claims, "claims", silver_path,
                                           partition_cols=["annee_sinistre", "mois_sinistre"])
        results["policies"] = write_silver(policies, "policies", silver_path)
        results["clients"]  = write_silver(clients,  "clients",  silver_path)

        log.complete(nb_records_out=-1)

    return results


if __name__ == "__main__":
    stats = run_bronze_to_silver()
    print(f"Bronze → Silver terminé : {stats}")
