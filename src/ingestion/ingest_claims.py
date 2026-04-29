"""
Ingestion des sinistres (claims) vers la couche Bronze.

Lecture CSV multi-format, cast des types, ajout métadonnées d'ingestion,
validation schéma et écriture Delta Lake partitionnée.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ingestion.schema_validator import SchemaValidator
from src.utils.config import BRONZE_PATH, CLAIMS_CSV
from src.utils.logger import pipeline_step
from src.utils.spark_session import get_spark_session

# ── Schéma cible Bronze ────────────────────────────────────────────────────────

CLAIMS_BRONZE_SCHEMA = StructType([
    StructField("claim_id",            StringType(),  nullable=False),
    StructField("policy_id",           StringType(),  nullable=False),
    StructField("client_id",           StringType(),  nullable=False),
    StructField("type_sinistre",       StringType(),  nullable=False),
    StructField("date_sinistre",       StringType(),  nullable=True),
    StructField("date_declaration",    StringType(),  nullable=True),
    StructField("date_cloture",        StringType(),  nullable=True),
    StructField("montant_declare",     DoubleType(),  nullable=True),
    StructField("montant_provision",   DoubleType(),  nullable=True),
    StructField("montant_indemnise",   DoubleType(),  nullable=True),
    StructField("statut",              StringType(),  nullable=True),
    StructField("region_code",         StringType(),  nullable=True),
    StructField("description",         StringType(),  nullable=True),
    StructField("is_fraud_injected",   BooleanType(), nullable=True),
    StructField("fraud_rule_injected", StringType(),  nullable=True),
    # Metadata
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("source_system",       StringType(),    nullable=False),
    StructField("batch_id",            StringType(),    nullable=False),
    StructField("source_file",         StringType(),    nullable=True),
    StructField("annee_ingestion",     StringType(),    nullable=False),
    StructField("mois_ingestion",      StringType(),    nullable=False),
])

SOURCE_SYSTEM = "raw_csv_claims"


def read_claims_csv(
    spark: SparkSession,
    path: str = CLAIMS_CSV,
    encoding: str = "UTF-8",
) -> DataFrame:
    """Lit le fichier CSV des sinistres avec gestion multi-encodage.

    Args:
        spark: SparkSession active.
        path: Chemin vers le fichier CSV.
        encoding: Encodage du fichier (UTF-8 par défaut).

    Returns:
        DataFrame brut avec tous les champs en StringType.
    """
    return (
        spark.read
        .option("header", "true")
        .option("encoding", encoding)
        .option("multiLine", "true")
        .option("escape", '"')
        .option("nullValue", "")
        .option("nanValue", "NaN")
        .csv(path)
    )


def cast_claims_types(df: DataFrame) -> DataFrame:
    """Cast des types depuis le DataFrame brut string → types cibles.

    Args:
        df: DataFrame brut (tous champs String).

    Returns:
        DataFrame avec types corrects.
    """
    return (
        df
        .withColumn("montant_declare",   F.col("montant_declare").cast(DoubleType()))
        .withColumn("montant_provision", F.col("montant_provision").cast(DoubleType()))
        .withColumn("montant_indemnise", F.col("montant_indemnise").cast(DoubleType()))
        .withColumn("is_fraud_injected", F.col("is_fraud_injected").cast(BooleanType()))
        # Normalisation strings
        .withColumn("claim_id",       F.trim(F.col("claim_id")))
        .withColumn("policy_id",      F.trim(F.col("policy_id")))
        .withColumn("client_id",      F.trim(F.col("client_id")))
        .withColumn("type_sinistre",  F.trim(F.lower(F.col("type_sinistre"))))
        .withColumn("statut",         F.trim(F.lower(F.col("statut"))))
        .withColumn("region_code",    F.trim(F.col("region_code")))
    )


def add_ingestion_metadata(
    df: DataFrame,
    batch_id: str,
    source_file: str = CLAIMS_CSV,
) -> DataFrame:
    """Enrichit le DataFrame avec les métadonnées d'ingestion.

    Args:
        df: DataFrame typé.
        batch_id: Identifiant unique du batch.
        source_file: Chemin du fichier source.

    Returns:
        DataFrame enrichi.
    """
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


def write_bronze_claims(
    df: DataFrame,
    output_path: str = BRONZE_PATH,
    mode: str = "append",
) -> int:
    """Écrit la couche Bronze en format Delta Lake partitionné.

    Args:
        df: DataFrame prêt à écrire.
        output_path: Chemin racine de la couche Bronze.
        mode: Mode d'écriture Delta ('append' ou 'overwrite').

    Returns:
        Nombre de lignes écrites.
    """
    target = f"{output_path}/claims"
    n = df.count()
    (
        df.write
        .format("delta")
        .mode(mode)
        .partitionBy("annee_ingestion", "mois_ingestion")
        .option("mergeSchema", "true")
        .save(target)
    )
    return n


def ingest_claims(
    spark: Optional[SparkSession] = None,
    source_path: str = CLAIMS_CSV,
    output_path: str = BRONZE_PATH,
    batch_id: Optional[str] = None,
    mode: str = "append",
) -> dict[str, int]:
    """Pipeline complet d'ingestion des sinistres vers Bronze.

    Lit → caste → valide → enrichit métadonnées → écrit Delta.

    Args:
        spark: SparkSession (créée si None).
        source_path: Chemin CSV source.
        output_path: Chemin Bronze cible.
        batch_id: Identifiant batch (généré si None).
        mode: Mode d'écriture Delta.

    Returns:
        Dictionnaire {'n_in': int, 'n_valid': int, 'n_rejected': int}.
    """
    _spark = spark or get_spark_session()
    _batch_id = batch_id or str(uuid.uuid4())[:8]

    with pipeline_step("ingest_claims", batch_id=_batch_id) as log:
        log.info("Reading claims CSV", path=source_path)
        raw_df = read_claims_csv(_spark, source_path)
        n_in = raw_df.count()
        log.info("Raw records loaded", nb_records_in=n_in)

        typed_df = cast_claims_types(raw_df)

        validator = SchemaValidator("claims", _batch_id)
        valid_df = validator.validate_spark(typed_df)
        n_valid = valid_df.count()

        enriched_df = add_ingestion_metadata(valid_df, _batch_id, source_path)

        n_written = write_bronze_claims(enriched_df, output_path, mode)
        log.complete(nb_records_out=n_written, nb_rejected=n_in - n_valid)

    return {
        "n_in":       n_in,
        "n_valid":    n_valid,
        "n_rejected": n_in - n_valid,
    }


if __name__ == "__main__":
    result = ingest_claims()
    print(f"Ingestion terminée : {result}")
