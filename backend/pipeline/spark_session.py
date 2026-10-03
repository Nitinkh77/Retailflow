import os
import sys
from pyspark.sql import SparkSession

def get_spark(app="RetailFlow"):
    # Spark's Python workers default to "python3", which on Windows may be a Microsoft Store stub.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (SparkSession.builder.master("local[*]").appName(app)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")   # Spark 4 default is strict; we want bad casts -> null -> rejected
            .config("spark.ui.showConsoleProgress", "false").getOrCreate())