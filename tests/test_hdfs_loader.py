from __future__ import annotations

from pathlib import Path

from ingestion.hdfs_loader import (
    hdfs_put_command,
    parse_hdfs_ls_sizes,
    plan_uploads,
    to_container_path,
)


def test_plan_uploads_skips_same_size_files(tmp_path: Path):
    f1 = tmp_path / "2026-01-01.csv"
    f2 = tmp_path / "2026-01-02.csv"
    f1.write_text("a" * 10)
    f2.write_text("b" * 20)

    existing_sizes = {"2026-01-01.csv": 10, "2026-01-02.csv": 999}

    plan = plan_uploads([f1, f2], existing_sizes)

    assert plan.skipped_same_size == [f1]
    assert plan.to_upload == [f2]


def test_plan_uploads_all_new_when_nothing_exists(tmp_path: Path):
    f1 = tmp_path / "2026-01-01.csv"
    f1.write_text("a")

    plan = plan_uploads([f1], existing_sizes={})

    assert plan.to_upload == [f1]
    assert plan.skipped_same_size == []


def test_to_container_path_maps_nested_host_dir():
    host_root = Path("C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    local_file = host_root / "2026-01-01.csv"

    result = to_container_path(local_file, host_root, "/external_data/data_Q1_2026")

    assert result == "/external_data/data_Q1_2026/2026-01-01.csv"


def test_hdfs_put_command_sets_replication_and_overwrite():
    cmd = hdfs_put_command("/external_data/data_Q1_2026/2026-01-01.csv", "/smart-drive/bronze", "2026-01-01.csv", replication=1)

    assert cmd == [
        "hdfs",
        "dfs",
        "-D",
        "dfs.replication=1",
        "-put",
        "-f",
        "/external_data/data_Q1_2026/2026-01-01.csv",
        "/smart-drive/bronze/2026-01-01.csv",
    ]


def test_parse_hdfs_ls_sizes_ignores_directories_and_header():
    ls_output = (
        "Found 3 items\n"
        "drwxr-xr-x   - root supergroup          0 2026-09-30 00:00 /smart-drive/bronze/year=2026\n"
        "-rw-r--r--   1 root supergroup       1234 2026-09-30 00:00 /smart-drive/bronze/2026-01-01.csv\n"
        "-rw-r--r--   1 root supergroup       5678 2026-09-30 00:00 /smart-drive/bronze/2026-01-02.csv\n"
    )

    sizes = parse_hdfs_ls_sizes(ls_output)

    assert sizes == {"2026-01-01.csv": 1234, "2026-01-02.csv": 5678}
