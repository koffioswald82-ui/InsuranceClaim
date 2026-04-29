"""Tests unitaires — transformations Bronze → Silver."""
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)

from src.transformation.bronze_to_silver import (
    add_silver_columns,
    clean_claims,
    compute_declaration_delay,
    deduplicate,
    standardize_dates,
)


# ── deduplicate ────────────────────────────────────────────────────────────────

class TestDeduplicate:

    def test_removes_duplicates_keeps_latest(self, spark: SparkSession):
        schema = StructType([
            StructField("claim_id", StringType()),
            StructField("montant_declare", DoubleType()),
            StructField("ingestion_timestamp", StringType()),
        ])
        data = [
            ("C001", 1000.0, "2024-01-01T00:00:00"),
            ("C001", 2000.0, "2024-01-02T00:00:00"),  # plus récent
            ("C002", 500.0,  "2024-01-01T00:00:00"),
        ]
        df = spark.createDataFrame(data, schema)
        result = deduplicate(df, ["claim_id"])

        assert result.count() == 2
        c001 = result.filter(F.col("claim_id") == "C001").collect()[0]
        assert c001["montant_declare"] == 2000.0

    def test_no_duplicates_unchanged(self, spark: SparkSession):
        schema = StructType([
            StructField("claim_id", StringType()),
            StructField("ingestion_timestamp", StringType()),
        ])
        data = [("C001", "2024-01-01T00:00:00"), ("C002", "2024-01-01T00:00:00")]
        df = spark.createDataFrame(data, schema)
        result = deduplicate(df, ["claim_id"])
        assert result.count() == 2


# ── clean_claims ───────────────────────────────────────────────────────────────

class TestCleanClaims:

    def _make_claims(self, spark, rows):
        schema = StructType([
            StructField("montant_declare",   DoubleType()),
            StructField("montant_provision", DoubleType()),
            StructField("montant_indemnise", DoubleType()),
            StructField("description",       StringType()),
            StructField("statut",            StringType()),
            StructField("type_sinistre",     StringType()),
        ])
        return spark.createDataFrame(rows, schema)

    def test_null_amounts_filled_with_zero(self, spark: SparkSession):
        df = self._make_claims(spark, [(None, None, None, None, "ouvert", "vol")])
        result = clean_claims(df).collect()[0]
        assert result["montant_declare"]   == 0.0
        assert result["montant_provision"] == 0.0
        assert result["montant_indemnise"] == 0.0

    def test_statut_lowercased_and_trimmed(self, spark: SparkSession):
        df = self._make_claims(spark, [(100.0, 0.0, 0.0, "desc", "  OUVERT  ", "vol")])
        result = clean_claims(df).collect()[0]
        assert result["statut"] == "ouvert"

    def test_null_statut_becomes_inconnu(self, spark: SparkSession):
        df = self._make_claims(spark, [(100.0, 0.0, 0.0, "desc", None, "vol")])
        result = clean_claims(df).collect()[0]
        assert result["statut"] == "inconnu"


# ── standardize_dates ──────────────────────────────────────────────────────────

class TestStandardizeDates:

    def test_iso_format_parsed(self, spark: SparkSession):
        df = spark.createDataFrame([("2024-06-15",)], ["date_sinistre"])
        result = standardize_dates(df, ["date_sinistre"])
        val = result.collect()[0]["date_sinistre"]
        assert val == date(2024, 6, 15)

    def test_french_format_parsed(self, spark: SparkSession):
        df = spark.createDataFrame([("15/06/2024",)], ["date_sinistre"])
        result = standardize_dates(df, ["date_sinistre"])
        val = result.collect()[0]["date_sinistre"]
        assert val == date(2024, 6, 15)

    def test_missing_col_ignored(self, spark: SparkSession):
        df = spark.createDataFrame([("x",)], ["autre_col"])
        result = standardize_dates(df, ["date_sinistre"])
        assert "date_sinistre" not in result.columns


# ── compute_declaration_delay ──────────────────────────────────────────────────

class TestDeclarationDelay:

    def _make_df(self, spark, sinistre, declaration):
        schema = StructType([
            StructField("date_sinistre",    DateType()),
            StructField("date_declaration", DateType()),
        ])
        return spark.createDataFrame([(sinistre, declaration)], schema)

    def test_positive_delay_computed(self, spark: SparkSession):
        df = self._make_df(spark, date(2024, 1, 1), date(2024, 1, 11))
        result = compute_declaration_delay(df).collect()[0]
        assert result["delai_declaration_jours"] == 10

    def test_negative_delay_nullified(self, spark: SparkSession):
        df = self._make_df(spark, date(2024, 1, 11), date(2024, 1, 1))
        result = compute_declaration_delay(df).collect()[0]
        assert result["delai_declaration_jours"] is None

    def test_same_day_is_zero(self, spark: SparkSession):
        df = self._make_df(spark, date(2024, 1, 1), date(2024, 1, 1))
        result = compute_declaration_delay(df).collect()[0]
        assert result["delai_declaration_jours"] == 0


# ── add_silver_columns ─────────────────────────────────────────────────────────

class TestAddSilverColumns:

    def test_partitioning_columns_added(self, spark: SparkSession):
        schema = StructType([StructField("date_sinistre", DateType())])
        df = spark.createDataFrame([(date(2023, 7, 15),)], schema)
        result = add_silver_columns(df).collect()[0]
        assert result["annee_sinistre"] == "2023"
        assert result["mois_sinistre"]  == "07"
        assert result["trimestre"]      == "2023-Q3"

    def test_silver_processed_at_present(self, spark: SparkSession):
        schema = StructType([StructField("date_sinistre", DateType())])
        df = spark.createDataFrame([(date(2024, 1, 1),)], schema)
        result = add_silver_columns(df)
        assert "silver_processed_at" in result.columns
