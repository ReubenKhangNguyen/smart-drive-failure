"""Print a compact Spark SQL summary from the Silver SMART table for demonstrations."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("smart-drive-spark-sql-demo").getOrCreate()
silver = spark.read.parquet("hdfs://namenode:9000/smart-drive/silver/daily")
silver.createOrReplaceTempView("smart_daily")
spark.sql("SELECT COUNT(*) AS total_records, SUM(failure) AS failure_records FROM smart_daily").show()
spark.sql("SELECT date, SUM(failure) AS failures FROM smart_daily GROUP BY date ORDER BY date").show(14, truncate=False)
spark.stop()
