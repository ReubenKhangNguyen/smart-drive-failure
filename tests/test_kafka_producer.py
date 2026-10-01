from __future__ import annotations

import json
from pathlib import Path

from ingestion.kafka_producer import read_csv_rows, row_to_message, stream_file

HEADER = "date,serial_number,model,capacity_bytes,failure,smart_5_raw,smart_9_raw,smart_187_raw,smart_188_raw,smart_194_raw,smart_197_raw,smart_198_raw,smart_199_raw"


def _write_csv(path: Path, rows):
    lines = [HEADER] + rows
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_read_csv_rows_yields_dicts(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(path, ["2026-01-01,SN001,ModelA,1000,0,0,100,0,0,30,0,0,0"])

    rows = list(read_csv_rows(path))

    assert len(rows) == 1
    assert rows[0]["serial_number"] == "SN001"


def test_row_to_message_is_valid_json_bytes():
    message = row_to_message({"serial_number": "SN001", "date": "2026-01-01"})

    decoded = json.loads(message.decode("utf-8"))
    assert decoded == {"serial_number": "SN001", "date": "2026-01-01"}


def test_stream_file_sends_every_row_with_delay(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(
        path,
        [
            "2026-01-01,SN001,ModelA,1000,0,0,100,0,0,30,0,0,0",
            "2026-01-01,SN002,ModelA,1000,0,0,100,0,0,30,0,0,0",
        ],
    )

    sent_messages = []
    sleeps = []

    count = stream_file(path, send=sent_messages.append, delay_seconds=0.01, sleep=sleeps.append)

    assert count == 2
    assert len(sent_messages) == 2
    assert len(sleeps) == 2
    assert json.loads(sent_messages[0])["serial_number"] == "SN001"
