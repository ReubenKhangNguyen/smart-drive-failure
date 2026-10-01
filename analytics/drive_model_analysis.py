from __future__ import annotations

from pyspark.sql import DataFrame


def model_population_summary(df: DataFrame, view_name: str = "silver_daily") -> DataFrame:
    """Distinct drive count, avg capacity, and failure count per model (Spark SQL, buoc 5)."""
    df.createOrReplaceTempView(view_name)
    query = """
        SELECT model,
               COUNT(DISTINCT serial_number) AS drive_count,
               AVG(capacity_bytes) AS avg_capacity_bytes,
               SUM(failure) AS failures
        FROM {view_name}
        GROUP BY model
        ORDER BY drive_count DESC
    """.format(view_name=view_name)
    return df.sparkSession.sql(query)
