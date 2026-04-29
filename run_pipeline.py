"""Lance le pipeline complet Bronze → Silver → Gold."""
import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from src.utils.spark_session import get_spark_session
from src.ingestion.ingest_claims import ingest_claims
from src.ingestion.ingest_policies import ingest_policies
from src.transformation.bronze_to_silver import run_bronze_to_silver
from src.transformation.silver_to_gold import run_silver_to_gold
from src.transformation.fraud_flags import run_fraud_flags
from src.business_views.loss_ratio import compute_loss_ratio
from src.business_views.churn_score import compute_churn_scores

spark = get_spark_session()

print("--- Ingestion ---")
ingest_claims(spark=spark)
ingest_policies(spark=spark)

# Ingestion clients + payments (lecture directe → Bronze)
from src.utils.config import BRONZE_PATH, CLIENTS_PARQUET, PAYMENTS_CSV
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
meta = {
    "ingestion_timestamp": now.isoformat(),
    "source_system":       "raw_parquet_clients",
    "batch_id":            "clients_boot",
    "source_file":         "clients.parquet",
    "annee_ingestion":     str(now.year),
    "mois_ingestion":      f"{now.month:02d}",
}
from pyspark.sql import functions as F

print("Ingestion clients → Bronze")
clients_df = spark.read.parquet(CLIENTS_PARQUET)
for k, v in meta.items():
    clients_df = clients_df.withColumn(k, F.lit(v))
clients_df.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(f"{BRONZE_PATH}/clients")

print("Ingestion payments → Bronze")
meta["source_system"] = "raw_csv_payments"
meta["source_file"]   = "payments.csv"
payments_df = spark.read.option("header", "true").csv(PAYMENTS_CSV)
for k, v in meta.items():
    payments_df = payments_df.withColumn(k, F.lit(v))
payments_df.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(f"{BRONZE_PATH}/payments")

print("--- Bronze → Silver ---")
run_bronze_to_silver(spark=spark)

print("--- Silver → Gold ---")
run_silver_to_gold(spark=spark)

print("--- Fraud Detection ---")
run_fraud_flags(spark=spark)

print("--- Business Views ---")
compute_loss_ratio(spark=spark)
compute_churn_scores(spark=spark)

print("Pipeline terminé.")
