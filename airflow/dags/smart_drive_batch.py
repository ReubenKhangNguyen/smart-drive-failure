"""Airflow DAG that runs the SMART Drive batch stages on the Spark cluster."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 "
    "--conf spark.driver.host=airflow-scheduler "
    "--conf spark.driver.bindAddress=0.0.0.0 "
)

with DAG(
    dag_id="smart_drive_batch",
    description="ETL, Gold survey and K-Means health segmentation for Backblaze SMART data.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smart-drive", "spark", "hdfs"],
) as dag:
    etl = BashOperator(
        task_id="bronze_to_silver_etl",
        bash_command=SPARK_SUBMIT + "/opt/project/processing/spark_jobs/smart_etl.py",
    )
    survey = BashOperator(
        task_id="silver_to_gold_survey",
        bash_command=SPARK_SUBMIT + "/opt/project/analytics/survey_gold.py",
    )
    cluster = BashOperator(
        task_id="kmeans_health_clusters",
        bash_command=SPARK_SUBMIT + "/opt/project/analytics/kmeans_health_clusters.py --k 3",
    )

    etl >> survey >> cluster
