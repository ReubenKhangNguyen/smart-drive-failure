from __future__ import annotations

import importlib.util
import io
import json
import urllib.error
from pathlib import Path

import pytest

from ingestion.bronze_verifier import UPLOAD_HINT, check_listing, describe, expected_dates, list_status, parse_liststatus

ROOT = Path(__file__).resolve().parent.parent


def _payload(names_and_sizes):
    return {"FileStatuses": {"FileStatus": [
        {"pathSuffix": name, "length": size, "type": "FILE"} for name, size in names_and_sizes]}}


def test_expected_dates_covers_the_whole_inclusive_range():
    dates = expected_dates("2026-01-01", "2026-03-31")

    assert len(dates) == 90 and dates[0] == "2026-01-01" and dates[-1] == "2026-03-31"
    assert expected_dates("2026-02-27", "2026-03-02") == ["2026-02-27", "2026-02-28", "2026-03-01", "2026-03-02"]  # not a leap year


def test_parse_liststatus_keeps_files_only():
    payload = {"FileStatuses": {"FileStatus": [
        {"pathSuffix": "2026-01-01.csv", "length": 133009965, "type": "FILE"},
        {"pathSuffix": "subdir", "length": 0, "type": "DIRECTORY"}]}}

    assert parse_liststatus(payload) == {"2026-01-01.csv": 133009965}
    assert parse_liststatus({}) == {}


def test_check_listing_accepts_a_complete_bronze():
    dates = ["2026-01-01", "2026-01-02"]

    check = check_listing({"2026-01-01.csv": 10, "2026-01-02.csv": 20, "extra.txt": 5}, dates)

    assert check.ok and check.present == 2 and check.expected == 2 and describe(check) == "Bronze: 2/2 ngày có file"


def test_check_listing_reports_missing_and_empty_days():
    dates = ["2026-01-01", "2026-01-02", "2026-01-03"]

    check = check_listing({"2026-01-01.csv": 10, "2026-01-03.csv": 0}, dates)

    assert not check.ok and check.missing == ["2026-01-02"] and check.empty == ["2026-01-03"] and check.present == 2
    assert "thiếu: 2026-01-02" in describe(check) and "rỗng: 2026-01-03" in describe(check)


def test_the_hint_says_the_dag_verifies_and_the_upload_is_a_manual_host_step():
    assert "upload_to_hdfs.py" in UPLOAD_HINT and "không nạp" in UPLOAD_HINT and "host" in UPLOAD_HINT


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_list_status_builds_the_webhdfs_url_and_decodes_json():
    seen = {}

    def opener(url, timeout):
        seen["url"] = url
        return _Response(json.dumps(_payload([("2026-01-01.csv", 7)])).encode("utf-8"))

    result = list_status("http://namenode:9870/", "/smart-drive/bronze/year=2026/quarter=Q1", opener=opener)

    assert seen["url"] == "http://namenode:9870/webhdfs/v1/smart-drive/bronze/year=2026/quarter=Q1?op=LISTSTATUS"
    assert parse_liststatus(result) == {"2026-01-01.csv": 7}


def test_list_status_treats_a_missing_directory_as_empty_but_not_other_errors():
    def not_found(url, timeout):
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    def server_error(url, timeout):
        raise urllib.error.HTTPError(url, 500, "Boom", {}, None)

    assert list_status("http://nn:9870", "/x", opener=not_found) == {}
    with pytest.raises(urllib.error.HTTPError):
        list_status("http://nn:9870", "/x", opener=server_error)


def _load_script():
    spec = importlib.util.spec_from_file_location("verify_bronze", str(ROOT / "scripts" / "verify_bronze.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_verify_script_exit_codes_ok_incomplete_and_unreachable(monkeypatch, capsys):
    module = _load_script()
    full = {"{}.csv".format(d): 1 for d in expected_dates("2026-01-01", "2026-03-31")}
    monkeypatch.setattr("sys.argv", ["verify_bronze.py"])

    monkeypatch.setattr(module, "list_status", lambda url, path: _payload(list(full.items())))
    assert module.main() == 0

    partial = dict(full)
    del partial["2026-02-14.csv"]
    monkeypatch.setattr(module, "list_status", lambda url, path: _payload(list(partial.items())))
    assert module.main() == 1
    assert "2026-02-14" in capsys.readouterr().out

    def unreachable(url, path):
        raise OSError("namenode down")

    monkeypatch.setattr(module, "list_status", unreachable)
    assert module.main() == 3


def test_the_verifier_is_read_only_standard_library():
    for path in ("ingestion/bronze_verifier.py", "scripts/verify_bronze.py"):
        source = (ROOT / path).read_text(encoding="utf-8")
        assert "pyspark" not in source and "subprocess" not in source and "docker" not in source.lower().replace("the docker cli", ""), path
        assert "op=DELETE" not in source and "op=CREATE" not in source and "op=RENAME" not in source, path
