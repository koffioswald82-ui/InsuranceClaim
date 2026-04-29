"""Tests unitaires — règles de détection de fraude."""
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from src.transformation.fraud_flags import (
    rule_002_high_amount,
    rule_005_late_declaration,
)


# ── RULE_002 : high_amount ─────────────────────────────────────────────────────

class TestRule002HighAmount:

    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("claim_id",        StringType()),
            StructField("produit",         StringType()),
            StructField("montant_declare", DoubleType()),
        ])
        return spark.createDataFrame(rows, schema)

    def test_high_amount_flagged(self, spark: SparkSession):
        # [100, 100, 1000] → mean=400, std≈520, seuil(0.5σ)=660 → C3 flaggué
        data = [
            ("C1", "auto", 100.0),
            ("C2", "auto", 100.0),
            ("C3", "auto", 1000.0),
        ]
        df = self._make_df(spark, data)
        result = rule_002_high_amount(df, n_stddev=0.5)
        flags = {r["claim_id"]: r["flag_high_amount"] for r in result.collect()}
        assert flags["C1"] == 0
        assert flags["C3"] == 1

    def test_normal_amount_not_flagged(self, spark: SparkSession):
        data = [("C1", "auto", 100.0), ("C2", "auto", 110.0), ("C3", "auto", 90.0)]
        df = self._make_df(spark, data)
        result = rule_002_high_amount(df)
        for row in result.collect():
            assert row["flag_high_amount"] == 0


# ── RULE_005 : late_declaration ────────────────────────────────────────────────

class TestRule005LateDeclaration:

    def _make_df(self, spark, delays):
        schema = StructType([
            StructField("claim_id",                StringType()),
            StructField("delai_declaration_jours", IntegerType()),
        ])
        return spark.createDataFrame(
            [(f"C{i}", d) for i, d in enumerate(delays)], schema
        )

    def test_late_declaration_flagged(self, spark: SparkSession):
        df = self._make_df(spark, [10, 200])  # 200 > 180 jours
        result = rule_005_late_declaration(df)
        flags = {r["claim_id"]: r["flag_late_declaration"] for r in result.collect()}
        assert flags["C0"] == 0
        assert flags["C1"] == 1

    def test_null_delay_not_flagged(self, spark: SparkSession):
        schema = StructType([
            StructField("claim_id",                StringType()),
            StructField("delai_declaration_jours", IntegerType()),
        ])
        df = spark.createDataFrame([("C0", None)], schema)
        result = rule_005_late_declaration(df)
        assert result.collect()[0]["flag_late_declaration"] == 0

    def test_exactly_threshold_not_flagged(self, spark: SparkSession):
        df = self._make_df(spark, [180])  # exactement 180, pas > 180
        result = rule_005_late_declaration(df)
        assert result.collect()[0]["flag_late_declaration"] == 0
