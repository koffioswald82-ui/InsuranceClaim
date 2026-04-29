"""Configuration pytest racine — initialise HADOOP_HOME et PYSPARK_PYTHON avant tout import Spark."""
import os

os.environ.setdefault("HADOOP_HOME", "C:/hadoop")
os.environ.setdefault("hadoop.home.dir", "C:/hadoop")
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# Forcer les workers Spark à utiliser Anaconda (pas le Python 3.14 système)
_anaconda = r"C:\Users\koffi\anaconda3\python.exe"
os.environ["PYSPARK_PYTHON"] = _anaconda
os.environ["PYSPARK_DRIVER_PYTHON"] = _anaconda
