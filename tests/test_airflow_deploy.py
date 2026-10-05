from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
AIRFLOW_SERVICES = ["airflow-init", "airflow-webserver", "airflow-scheduler"]


def _compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def test_airflow_services_exist_only_under_the_orchestration_profile():
    services = _compose()["services"]

    for name in AIRFLOW_SERVICES:
        assert services[name]["profiles"] == ["orchestration"], name  # never started by a plain `docker compose up`
        assert services[name]["restart"] == "no"
    assert not any(n.startswith("airflow") for n in services if "profiles" not in services[n])


def test_the_airflow_ui_is_bound_to_localhost_only():
    ports = _compose()["services"]["airflow-webserver"]["ports"]

    assert len(ports) == 1 and ports[0].startswith("127.0.0.1:") and ports[0].endswith(":8080")
    assert "8081" in ports[0]


def test_no_service_mounts_the_docker_socket():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "docker.sock" not in text
    assert "docker.sock" not in (ROOT / "Dockerfile.airflow").read_text(encoding="utf-8")


def test_airflow_runs_sqlite_with_the_sequential_executor_and_a_named_volume():
    compose = _compose()
    env = compose["services"]["airflow-scheduler"]["environment"]

    assert env["AIRFLOW__CORE__EXECUTOR"] == "SequentialExecutor"
    assert env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].startswith("sqlite:////opt/airflow/")
    assert env["SPARK_DRIVER_HOST"] == "airflow-scheduler"
    assert compose["services"]["airflow-scheduler"]["hostname"] == "airflow-scheduler"  # executors connect back by this name
    assert "airflow_home" in compose["volumes"]
    volumes = compose["services"]["airflow-scheduler"]["volumes"]
    assert "airflow_home:/opt/airflow" in volumes  # the SQLite file is on a Docker volume, not a Windows folder
    assert "./dags:/opt/airflow/dags:ro" in volumes and ".:/opt/smart-drive:ro" in volumes
    assert "./config/hadoop:/opt/spark/conf/hadoop:ro" in volumes  # same HDFS client config as the Spark containers


def test_the_admin_password_comes_from_the_environment_and_is_never_written_down():
    compose_text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    init_env = _compose()["services"]["airflow-init"]["environment"]

    assert init_env["AIRFLOW_ADMIN_PASSWORD"] == "${AIRFLOW_ADMIN_PASSWORD:-}"  # interpolated from the environment/.env
    assert re.findall(r"--password\s+\S+", compose_text) == ['--password "$$AIRFLOW_ADMIN_PASSWORD"']
    assert 'test -n "$${AIRFLOW_ADMIN_PASSWORD:-}"' in compose_text  # an empty value stops the init with a message
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert re.search(r"^AIRFLOW_ADMIN_PASSWORD=$", example, flags=re.MULTILINE)  # placeholder, no value


def test_every_airflow_service_shares_one_image_and_pins_no_latest_tag():
    services = _compose()["services"]

    assert {services[n]["image"] for n in AIRFLOW_SERVICES} == {"smart-drive-failure-airflow:2.10.5"}
    dockerfile = (ROOT / "Dockerfile.airflow").read_text(encoding="utf-8")
    assert "ARG AIRFLOW_VERSION=2.10.5" in dockerfile
    assert "constraints-${AIRFLOW_VERSION}/constraints-3.8.txt" in dockerfile  # Python 3.8, like the Spark cluster
    assert "latest" not in dockerfile and "ARG BASE_IMAGE=smart-drive-failure-spark:3.5.1-numpy" in dockerfile


def test_airflow_is_installed_in_its_own_venv_and_the_venv_python_is_not_first_on_path():
    dockerfile = (ROOT / "Dockerfile.airflow").read_text(encoding="utf-8")

    assert "python3 -m venv /opt/airflow-venv" in dockerfile
    # tests/test_dag.py runs inside the Airflow image; 8.3.4 is the version the constraints file pins (8.3.3 failed to resolve)
    assert '"pytest==8.3.4"' in dockerfile
    path_lines = [l for l in dockerfile.splitlines() if l.startswith("ENV PATH=")]
    assert path_lines == ['ENV PATH="${PATH}:/opt/airflow-venv/bin"']  # appended last: python3 stays /usr/bin/python3


def test_the_dag_file_is_a_thin_wrapper_over_the_spec():
    source = (ROOT / "dags" / "smart_drive_pipeline_dag.py").read_text(encoding="utf-8")

    assert "schedule=None" in source and "max_active_runs=1" in source and '"retries": 0' in source
    assert "from pipeline.dag_spec import" in source
    assert "spark-submit" not in source and "pyspark" not in source  # the logic lives in the spec and the scripts


def test_the_webhdfs_url_used_by_verify_bronze_is_in_the_config():
    from config.settings import load_config

    assert load_config()["hdfs"]["namenode_web_url"] == "http://namenode:9870"


def test_the_new_quarter_dag_file_is_a_thin_wrapper_over_the_spec():
    source = (ROOT / "dags" / "smart_drive_new_quarter_dag.py").read_text(encoding="utf-8")

    assert "schedule=None" in source and "max_active_runs=1" in source and '"retries": 0' in source
    assert "from pipeline.dag_spec import" in source and "QUARTER_TASKS" in source
    assert "spark-submit" not in source and "pyspark" not in source
