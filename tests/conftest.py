"""SparkSession légère partagée pour les tests unitaires (sans Delta Lake)."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("axa-tests")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions",
                "-Djava.library.path=C:/hadoop/bin -Dhadoop.home.dir=C:/hadoop"
                " -XX:TieredStopAtLevel=1")
        .getOrCreate()
    )
