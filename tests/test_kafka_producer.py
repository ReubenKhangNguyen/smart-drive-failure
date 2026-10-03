from __future__ import annotations

import json
from pathlib import Path

from ingestion.kafka_producer import (
    REQUIRED_COLUMNS,
    DeliveryCounter,
    produce_dates,
    read_csv_rows,
    row_to_message,
    stream_file,
)
from processing.spark_jobs.smart_etl import REQUIRED_RAW_COLUMNS

HEADER = "date,serial_number,model,capacity_bytes,failure,smart_5_raw,smart_9_raw,smart_187_raw,smart_188_raw,smart_194_raw,smart_197_raw,smart_198_raw,smart_199_raw,extra_a,extra_b"


def _write_csv(path: Path, count: int, date: str = "2026-01-01"):
    rows = ["{},SN{:03d},ModelA,1000,0,0,100,,0,30,0,0,0,x,y".format(date, i) for i in range(count)]
    path.write_text("\n".join([HEADER] + rows) + "\n", encoding="utf-8")


def test_required_columns_stay_equal_to_the_batch_etl_columns():
    # the producer duplicates the list (it cannot import pyspark on the host); this guards against drift
    assert REQUIRED_COLUMNS == REQUIRED_RAW_COLUMNS


def test_read_csv_rows_yields_dicts(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(path, 1)

    rows = list(read_csv_rows(path))

    assert len(rows) == 1 and rows[0]["serial_number"] == "SN000"


def test_row_to_message_keeps_only_the_columns_the_consumer_reads():
    row = {c: "1" for c in REQUIRED_COLUMNS}
    row.update({"extra_a": "x" * 500, "extra_b": "y" * 500})

    decoded = json.loads(row_to_message(row).decode("utf-8"))

    assert list(decoded) == REQUIRED_COLUMNS  # exactly the 13 columns, in order, no extras
    assert len(row_to_message(row)) < 400


def test_a_column_missing_from_the_csv_becomes_null_not_an_error():
    decoded = json.loads(row_to_message({"date": "2026-01-01", "serial_number": "SN1"}).decode("utf-8"))

    assert decoded["smart_187_raw"] is None and decoded["serial_number"] == "SN1" and len(decoded) == len(REQUIRED_COLUMNS)


def test_row_to_message_can_still_send_every_column():
    decoded = json.loads(row_to_message({"a": "1", "b": "2"}, columns=None).decode("utf-8"))

    assert decoded == {"a": "1", "b": "2"}


def test_stream_file_sleeps_once_per_batch_not_once_per_message(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(path, 5)
    sent, sleeps = [], []

    count = stream_file(path, sent.append, batch_size=2, batch_delay=0.2, sleep=sleeps.append)

    assert count == 5 and len(sent) == 5
    assert sleeps == [0.2, 0.2]  # after the 2nd and the 4th message only; the 5th does not complete a batch
    assert json.loads(sent[0])["serial_number"] == "SN000"


def test_stream_file_with_batch_size_zero_never_sleeps(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(path, 4)
    sleeps = []

    assert stream_file(path, lambda m: None, batch_size=0, sleep=sleeps.append) == 4
    assert sleeps == []


def test_stream_file_stops_at_max_messages(tmp_path: Path):
    path = tmp_path / "2026-01-01.csv"
    _write_csv(path, 5)
    sent = []

    count = stream_file(path, sent.append, batch_size=2, batch_delay=0.0, max_messages=3, sleep=lambda s: None)

    assert count == 3 and len(sent) == 3


def test_produce_dates_streams_each_date_and_caps_the_total(tmp_path: Path):
    _write_csv(tmp_path / "2026-01-01.csv", 3, "2026-01-01")
    _write_csv(tmp_path / "2026-01-02.csv", 3, "2026-01-02")
    sent = []

    all_days = produce_dates(sent.append, tmp_path, ["2026-01-01", "2026-01-02"], batch_size=0, sleep=lambda s: None)
    capped = produce_dates(lambda m: None, tmp_path, ["2026-01-01", "2026-01-02"], batch_size=0, max_messages=4, sleep=lambda s: None)

    assert all_days == {"per_date": {"2026-01-01": 3, "2026-01-02": 3}, "total": 6} and len(sent) == 6
    assert capped == {"per_date": {"2026-01-01": 3, "2026-01-02": 1}, "total": 4}


class _FakeFuture:
    def __init__(self, ok: bool):
        self.ok = ok
        self.callbacks, self.errbacks = [], []

    def add_callback(self, fn):
        self.callbacks.append(fn)

    def add_errback(self, fn):
        self.errbacks.append(fn)

    def resolve(self):
        for fn in (self.callbacks if self.ok else self.errbacks):
            fn(object())


class _FakeProducer:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.futures = []
        self.topics = []

    def send(self, topic, message):
        self.topics.append(topic)
        future = _FakeFuture(self.outcomes.pop(0))
        self.futures.append(future)
        return future


def test_delivery_counter_separates_sent_acknowledged_and_failed():
    counter = DeliveryCounter()
    producer = _FakeProducer([True, True, False])
    send = counter.wrap(producer, "smart-events-x")

    for _ in range(3):
        send(b"{}")
    assert (counter.sent, counter.delivered, counter.failed) == (3, 0, 0)  # nothing acknowledged yet
    for future in producer.futures:
        future.resolve()

    assert (counter.sent, counter.delivered, counter.failed) == (3, 2, 1)
    assert producer.topics == ["smart-events-x"] * 3
