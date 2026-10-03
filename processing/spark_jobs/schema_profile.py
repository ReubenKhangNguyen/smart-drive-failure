from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import hdfs_uri, load_config


def list_csv_files(spark: SparkSession, dir_path: str) -> List[str]:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(dir_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(dir_path)
    statuses = fs.listStatus(path_obj)
    return sorted(str(s.getPath()) for s in statuses if str(s.getPath()).endswith(".csv"))


def read_header(spark: SparkSession, file_path: str) -> List[str]:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(file_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(file_path)
    stream = fs.open(path_obj)
    reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(stream))
    try:
        line = reader.readLine()
    finally:
        reader.close()
    return line.split(",") if line else []


@dataclass
class SchemaProfile:
    file_count: int
    reference_columns: List[str]
    drifted_files: Dict[str, str] = field(default_factory=dict)


def profile_schema(spark: SparkSession, dir_path: str) -> SchemaProfile:
    """List every CSV under dir_path and compare each header against the first file's
    header, reporting any file whose column set differs (schema drift)."""
    files = list_csv_files(spark, dir_path)
    if not files:
        return SchemaProfile(file_count=0, reference_columns=[])

    reference = read_header(spark, files[0])
    reference_set = set(reference)
    drifted: Dict[str, str] = {}

    for f in files[1:]:
        header = read_header(spark, f)
        header_set = set(header)
        if header_set != reference_set:
            missing = sorted(reference_set - header_set)
            extra = sorted(header_set - reference_set)
            drifted[f] = "missing={} extra={}".format(missing, extra)

    return SchemaProfile(file_count=len(files), reference_columns=reference, drifted_files=drifted)


def null_rate_report(df: DataFrame, columns: List[str]) -> Dict[str, float]:
    """Fraction of null values per column, computed with a single aggregation (no row collect)."""
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in columns}

    agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    row = df.select(*agg_exprs).collect()[0]
    return {c: row[c] / total for c in columns}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Liet ke cot va phat hien schema drift tren thu muc Bronze CSV")
    parser.add_argument(
        "--dir",
        default=hdfs_uri(load_config(), "bronze"),
        help="Duong dan HDFS hoac local toi thu muc chua CSV (mac dinh: Bronze trong config/project.yaml)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    spark = SparkSession.builder.appName("smart-schema-profile").getOrCreate()
    profile = profile_schema(spark, args.dir)

    print("Tong so file:", profile.file_count)
    print("Cot tham chieu ({}):".format(len(profile.reference_columns)))
    for col_name in profile.reference_columns:
        print(" -", col_name)

    if profile.drifted_files:
        print("File co schema drift:")
        for file_name, diff in profile.drifted_files.items():
            print(" -", file_name, ":", diff)
    else:
        print("Khong phat hien schema drift.")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
