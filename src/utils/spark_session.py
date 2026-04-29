"""
Factory SparkSession pour le pipeline AXA Claims Lakehouse.

Configure Delta Lake, les optimisations mémoire et le mode local vs cluster.
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from src.utils.config import (
    DELTA_VERSION,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER,
    ENV,
)
from src.utils.logger import get_logger

log = get_logger("spark_session")


def get_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    enable_delta: bool = True,
    enable_hive: bool = False,
    extra_configs: Optional[dict[str, str]] = None,
) -> SparkSession:
    """Crée ou récupère la SparkSession configurée pour le lakehouse.

    Configure automatiquement :
    - Delta Lake (ACID, time travel, schema evolution)
    - Optimisations mémoire (adaptive query execution, broadcast join)
    - Sérialisation Kryo
    - Mode local multi-core ou cluster selon la variable AXA_SPARK_MASTER

    Args:
        app_name: Nom de l'application Spark (défaut : AXA_SPARK_APP_NAME).
        master: URL du master Spark (défaut : AXA_SPARK_MASTER).
        enable_delta: Active le support Delta Lake (défaut : True).
        enable_hive: Active le support Hive Metastore (défaut : False).
        extra_configs: Configurations Spark supplémentaires.

    Returns:
        SparkSession configurée et prête à l'emploi.
    """
    _app_name = app_name or SPARK_APP_NAME
    _master   = master or SPARK_MASTER

    log.info("Building SparkSession", app_name=_app_name, master=_master, env=ENV)

    delta_jar = (
        f"io.delta:delta-spark_2.12:{DELTA_VERSION}"
        if enable_delta
        else None
    )

    builder = (
        SparkSession.builder
        .appName(_app_name)
        .master(_master)
        # Memory
        .config("spark.driver.memory",   SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        # Windows : forcer le chemin Hadoop en format Unix (évite parsing errors)
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.driver.extraJavaOptions",
                "-Djava.library.path=C:/hadoop/bin -Dhadoop.home.dir=C:/hadoop"
                " -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops"
                " -XX:+UseSerialGC -Xms128m")
        .config("spark.executor.extraJavaOptions",
                "-Djava.library.path=C:/hadoop/bin -Dhadoop.home.dir=C:/hadoop"
                " -XX:TieredStopAtLevel=1 -XX:-UseCompressedOops"
                " -XX:+UseSerialGC -Xms128m")
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        # Adaptive Query Execution
        .config("spark.sql.adaptive.enabled",                      "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",   "true")
        .config("spark.sql.adaptive.skewJoin.enabled",             "true")
        # Shuffle — minimum pour mode local
        .config("spark.sql.shuffle.partitions", "2")
        # Broadcast join
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")
        # Réduction mémoire JVM
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.executor.cores", "2")
        .config("spark.default.parallelism", "4")
    )

    if enable_delta:
        builder = (
            builder
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", delta_jar)
            # Schema evolution
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            # Optimize writes
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        )

    if enable_hive:
        builder = builder.enableHiveSupport()

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "SparkSession ready",
        version=spark.version,
        master=spark.sparkContext.master,
    )
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Arrête proprement la SparkSession.

    Args:
        spark: Session à arrêter.
    """
    log.info("Stopping SparkSession", app=spark.sparkContext.appName)
    spark.stop()
