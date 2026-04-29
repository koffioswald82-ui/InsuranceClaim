"""
Ingestion des polices d'assurance (JSON nested) vers la couche Bronze.

Lecture JSON nested, explosion du tableau `garanties[]`,
normalisation et enrichissement métadonnées.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ingestion.schema_validator import SchemaValidator
from src.utils.config import BRONZE_PATH, POLICIES_JSON
from src.utils.logger import pipeline_step
from src.utils.spark_session import get_spark_session

# ── Schéma JSON interne des garanties ─────────────────────────────────────────

GARANTIE_SCHEMA = StructType([
    StructField("code",    StringType(),  nullable=True),
    StructField("libelle", StringType(),  nullable=True),
    StructField("plafond", DoubleType(),  nullable=True),
    StructField("incluse", BooleanType(), nullable=True),
])

SOURCE_SYSTEM = "raw_json_policies"


def read_policies_json(
    spark: SparkSession,
    path: str = POLICIES_JSON,
) -> DataFrame:
    """Lit le fichier JSON des polices (tableau d'objets).

    Args:
        spark: SparkSession active.
        path: Chemin vers le fichier JSON.

    Returns:
        DataFrame brut avec garanties[] comme colonne Array<Struct>.
    """
    garanties_type = ArrayType(GARANTIE_SCHEMA)

    schema = StructType([
        StructField("policy_id",          StringType(), nullable=True),
        StructField("client_id",          StringType(), nullable=True),
        StructField("produit",            StringType(), nullable=True),
        StructField("date_souscription",  StringType(), nullable=True),
        StructField("date_echeance",      StringType(), nullable=True),
        StructField("prime_annuelle",     DoubleType(), nullable=True),
        StructField("prime_mensuelle",    DoubleType(), nullable=True),
        StructField("canal_vente",        StringType(), nullable=True),
        StructField("statut",             StringType(), nullable=True),
        StructField("region_code",        StringType(), nullable=True),
        StructField("franchise",          DoubleType(), nullable=True),
        StructField("plafond_garantie",   DoubleType(), nullable=True),
        StructField("garanties",          garanties_type, nullable=True),
    ])

    return (
        spark.read
        .option("multiLine", "true")
        .schema(schema)
        .json(path)
    )


def explode_garanties(df: DataFrame) -> DataFrame:
    """Explose le tableau garanties[] en lignes individuelles.

    Chaque ligne de la table résultante représente une garantie
    d'une police. Les polices sans garantie sont conservées via
    outer explode.

    Args:
        df: DataFrame avec colonne 'garanties' de type Array<Struct>.

    Returns:
        DataFrame avec colonnes garantie_* aplaties.
    """
    exploded = df.withColumn(
        "garantie", F.explode_outer(F.col("garanties"))
    ).drop("garanties")

    return (
        exploded
        .withColumn("garantie_code",    F.col("garantie.code"))
        .withColumn("garantie_libelle", F.col("garantie.libelle"))
        .withColumn("garantie_plafond", F.col("garantie.plafond"))
        .withColumn("garantie_incluse", F.col("garantie.incluse"))
        .drop("garantie")
    )


def normalize_policies(df: DataFrame) -> DataFrame:
    """Normalise les champs des polices.

    - Trim des chaînes
    - Lowercase cohérent sur produit, statut, canal_vente
    - Remplacement nulls business

    Args:
        df: DataFrame avec garanties explosées.

    Returns:
        DataFrame normalisé.
    """
    return (
        df
        .withColumn("policy_id",         F.trim(F.col("policy_id")))
        .withColumn("client_id",         F.trim(F.col("client_id")))
        .withColumn("produit",           F.trim(F.lower(F.col("produit"))))
        .withColumn("statut",            F.trim(F.lower(F.col("statut"))))
        .withColumn("canal_vente",       F.trim(F.lower(F.col("canal_vente"))))
        .withColumn("region_code",       F.trim(F.col("region_code")))
        .withColumn("garantie_code",     F.trim(F.lower(F.coalesce(F.col("garantie_code"), F.lit("inconnu")))))
        .withColumn("garantie_libelle",  F.trim(F.coalesce(F.col("garantie_libelle"), F.lit("Non renseigné"))))
        .withColumn("garantie_incluse",  F.coalesce(F.col("garantie_incluse"), F.lit(False)))
        .withColumn("franchise",         F.coalesce(F.col("franchise"), F.lit(0.0)))
    )


def add_ingestion_metadata_policies(
    df: DataFrame,
    batch_id: str,
    source_file: str = POLICIES_JSON,
) -> DataFrame:
    """Enrichit les polices avec les métadonnées d'ingestion.

    Args:
        df: DataFrame normalisé.
        batch_id: Identifiant du batch.
        source_file: Fichier source.

    Returns:
        DataFrame enrichi.
    """
    from pathlib import Path
    now = datetime.now(timezone.utc)
    return (
        df
        .withColumn("ingestion_timestamp", F.lit(now.isoformat()).cast(TimestampType()))
        .withColumn("source_system",       F.lit(SOURCE_SYSTEM))
        .withColumn("batch_id",            F.lit(batch_id))
        .withColumn("source_file",         F.lit(Path(source_file).name))
        .withColumn("annee_ingestion",     F.lit(str(now.year)))
        .withColumn("mois_ingestion",      F.lit(f"{now.month:02d}"))
    )


def write_bronze_policies(
    df: DataFrame,
    output_path: str = BRONZE_PATH,
    mode: str = "append",
) -> int:
    """Écrit les polices en Bronze Delta Lake.

    Args:
        df: DataFrame prêt à écrire.
        output_path: Chemin Bronze.
        mode: Mode Delta.

    Returns:
        Nombre de lignes écrites.
    """
    target = f"{output_path}/policies"
    (
        df.write
        .format("delta")
        .mode(mode)
        .partitionBy("annee_ingestion", "mois_ingestion")
        .option("mergeSchema", "true")
        .save(target)
    )
    return -1  # count skipped pour économiser mémoire


def ingest_policies(
    spark: Optional[SparkSession] = None,
    source_path: str = POLICIES_JSON,
    output_path: str = BRONZE_PATH,
    batch_id: Optional[str] = None,
    mode: str = "append",
) -> dict[str, int]:
    """Pipeline complet d'ingestion des polices vers Bronze.

    Lit JSON → explose garanties → normalise → valide → enrichit → écrit Delta.

    Args:
        spark: SparkSession (créée si None).
        source_path: Chemin JSON source.
        output_path: Chemin Bronze cible.
        batch_id: Identifiant batch.
        mode: Mode Delta.

    Returns:
        Dictionnaire {'n_in', 'n_valid', 'n_rejected', 'n_garanties'}.
    """
    _spark = spark or get_spark_session()
    _batch_id = batch_id or str(uuid.uuid4())[:8]

    with pipeline_step("ingest_policies", batch_id=_batch_id) as log:
        log.info("Reading policies JSON", path=source_path)
        raw_df = read_policies_json(_spark, source_path)
        n_policies = raw_df.count()
        log.info("Policies loaded", n_policies=n_policies)

        exploded_df = explode_garanties(raw_df)

        normalized_df = normalize_policies(exploded_df)

        validator = SchemaValidator("policies", _batch_id)
        valid_df = validator.validate_spark(normalized_df)

        enriched_df = add_ingestion_metadata_policies(valid_df, _batch_id, source_path)

        n_written = write_bronze_policies(enriched_df, output_path, mode)
        log.complete(
            nb_records_out=n_written,
            n_source_policies=n_policies,
        )

    return {
        "n_policies":  n_policies,
        "n_written":   n_written,
    }


if __name__ == "__main__":
    result = ingest_policies()
    print(f"Ingestion polices terminée : {result}")
