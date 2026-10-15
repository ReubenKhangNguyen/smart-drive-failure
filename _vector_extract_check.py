from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql.column import Column
from pyspark.sql.functions import _to_java_column, col
spark = SparkSession.builder.master("local[1]").appName("vector-extract-check").getOrCreate()
df = spark.createDataFrame([(Vectors.dense(0.2, 0.8),), (Vectors.sparse(2, {0: 1.0}),), (Vectors.sparse(2, {1: 0.75}),)], ["probability"])
array_col = Column(spark.sparkContext._jvm.org.apache.spark.ml.functions.vector_to_array(_to_java_column(col("probability")), "float64"))
df.select(array_col[1].alias("positive_probability")).show(truncate=False)
spark.stop()
