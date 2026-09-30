from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame


def afr_by_group(df: DataFrame, group_cols: List[str], view_name: str = "silver_daily") -> DataFrame:
    """Annualized failure rate = failures / drive-days * 365, via Spark SQL (bat buoc buoc 5)."""
    df.createOrReplaceTempView(view_name)
    group_expr = ", ".join(group_cols)
    query = """
        SELECT {group_expr},
               COUNT(*) AS drive_days,
               SUM(failure) AS failures,
               SUM(failure) / COUNT(*) * 365 AS afr
        FROM {view_name}
        GROUP BY {group_expr}
        ORDER BY afr DESC
    """.format(group_expr=group_expr, view_name=view_name)
    return df.sparkSession.sql(query)


def overall_afr(df: DataFrame, view_name: str = "silver_daily") -> float:
    df.createOrReplaceTempView(view_name)
    row = df.sparkSession.sql(
        "SELECT SUM(failure) / COUNT(*) * 365 AS afr FROM {}".format(view_name)
    ).collect()[0]
    return row["afr"] or 0.0
