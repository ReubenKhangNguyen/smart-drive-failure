"""Evidence report of the Kafka streaming demo (artifacts/reports/streaming_demo.md). Pure standard library: parses
`docker stats` text and formats the markdown from the producer/consumer summaries the demo collected."""
from __future__ import annotations

import re
from typing import Any, Dict, List

_STATS_LINE = re.compile(r"^(\S+)\s+([0-9.]+)\s*(KiB|MiB|GiB|B)\b")
_TO_MIB = {"B": 1.0 / (1024 * 1024), "KiB": 1.0 / 1024, "MiB": 1.0, "GiB": 1024.0}


def parse_docker_stats(text: str) -> Dict[str, float]:
    """`docker stats --no-stream --format "{{.Name}} {{.MemUsage}}"` text -> {container: MiB in use}."""
    out = {}  # type: Dict[str, float]
    for line in text.splitlines():
        match = _STATS_LINE.match(line.strip())
        if match:
            out[match.group(1)] = float(match.group(2)) * _TO_MIB[match.group(3)]
    return out


def memory_summary(samples: List[Dict[str, float]]) -> Dict[str, Any]:
    """Peak total over all samples and the peak of each container."""
    if not samples:
        return {"samples": 0, "peak_total_mib": None, "peak_by_container": {}}
    peak_by = {}  # type: Dict[str, float]
    for sample in samples:
        for name, value in sample.items():
            peak_by[name] = max(peak_by.get(name, 0.0), value)
    return {
        "samples": len(samples),
        "peak_total_mib": max(sum(s.values()) for s in samples),
        "peak_by_container": dict(sorted(peak_by.items(), key=lambda kv: -kv[1])),
    }


def total_mib(sample: Dict[str, float]) -> float:
    return sum(sample.values())


def _check(ok: bool) -> str:
    return "ĐẠT" if ok else "KHÔNG KHỚP"


def format_report(run: Dict[str, Any]) -> str:
    producer, consumer = run["producer"], run["consumer"]
    comparison = consumer.get("silver_comparison")
    memory = run.get("memory", {})
    sent, delivered, failed = producer["sent"], producer["delivered"], producer["failed"]
    output_rows = consumer["output_rows"]

    lines = ["# Demo streaming Kafka (bước 8 của giảng viên) — {}\n".format(run["run_id"])]
    lines.append(
        "Lớp trình diễn bổ sung: **batch pipeline (Bronze → Silver → Gold) vẫn là nguồn chính cho phân tích và huấn luyện**; "
        "Kafka chỉ mô phỏng dữ liệu SMART đổ về hằng ngày và chấm làm sạch gần thời gian thực. Chạy đúng 1 ngày dữ liệu "
        "({}), message chỉ gồm 13 cột consumer dùng (~314 byte, thay vì ~4,885 byte nếu gửi cả 197 cột), phát theo lô "
        "{:,} message mỗi {} giây.\n".format(", ".join(producer["dates"]), producer["batch_size"], producer["batch_delay"]))

    lines += ["## Kiểm chứng\n", "| Kiểm tra | Giá trị | Kết quả |", "|---|---|---|"]
    lines.append("| Producer gửi / broker xác nhận / lỗi | {:,} / {:,} / {:,} | {} |".format(
        sent, delivered, failed, _check(sent == delivered and failed == 0)))
    lines.append("| Consumer đọc từ Kafka | {:,} dòng | {} |".format(
        consumer["streamed_input_rows"], _check(consumer["streamed_input_rows"] == sent)))
    lines.append("| Dòng Parquet trong `streaming_output/{}` (đã qua `cast_and_clean_columns`) | {:,} | {} |".format(
        run["run_id"], output_rows, _check(output_rows == sent)))
    if comparison:
        s, b = comparison["streamed"], comparison["silver"]
        lines.append("| So với Silver {}: dòng | {:,} so với {:,} | {} |".format(
            comparison["date"], s["rows"], b["rows"], _check(s["rows"] == b["rows"])))
        lines.append("| So với Silver: số serial khác nhau | {:,} so với {:,} | {} |".format(
            s["serials"], b["serials"], _check(s["serials"] == b["serials"])))
        lines.append("| So với Silver: tổng `failure` | {:,} so với {:,} | {} |".format(
            s["failures"], b["failures"], _check(s["failures"] == b["failures"])))
    lines.append("")

    lines += ["## Micro-batch của Spark Structured Streaming\n", "| Batch | Số dòng | Thời gian xử lý (ms) | Dòng/giây |", "|---|---|---|---|"]
    for batch in consumer["micro_batches"]:
        rate = batch.get("processedRowsPerSecond")
        lines.append("| {} | {:,} | {} | {} |".format(
            batch["batchId"], batch["numInputRows"], batch.get("triggerExecutionMs", "—"),
            "{:,.0f}".format(rate) if rate else "—"))
    lines.append("\nConsumer dừng vì: `{}` (không có dữ liệu mới trong khoảng chờ, hoặc hết thời gian tối đa).\n".format(
        consumer["stopped_because"]))

    lines += ["## Thời gian\n",
              "- Producer: {} giây; consumer (từ lúc khởi động đến khi dừng): {} giây.".format(producer["seconds"], consumer["seconds"]),
              "- Topic `{}`, `maxOffsetsPerTrigger` = {:,}, executor mặc định (không đổi cỡ).\n".format(
                  consumer["topic"], consumer["max_offsets_per_trigger"])]

    if memory:
        lines += ["## RAM (docker stats)\n"]
        if memory.get("before_kafka_mib") is not None:
            lines.append("- Trước khi bật Kafka/ZooKeeper: {:,.0f} MiB (tổng các container).".format(memory["before_kafka_mib"]))
        if memory.get("after_kafka_mib") is not None:
            lines.append("- Sau khi Kafka/ZooKeeper chạy, chưa có tải: {:,.0f} MiB.".format(memory["after_kafka_mib"]))
        if memory.get("peak_total_mib") is not None:
            lines.append("- **Đỉnh trong lúc chạy demo: {:,.0f} MiB ({:.2f} GiB)** trên {} mẫu; giới hạn Docker {:,.0f} MiB.".format(
                memory["peak_total_mib"], memory["peak_total_mib"] / 1024.0, memory["samples"], memory.get("docker_limit_mib", 0)))
            lines += ["", "| Container | Đỉnh (MiB) |", "|---|---|"]
            for name, value in memory["peak_by_container"].items():
                lines.append("| {} | {:,.0f} |".format(name, value))
        lines.append("")

    lines += ["## Giới hạn\n",
              "- Đây là mô phỏng luồng trên một ngày dữ liệu có sẵn, không phải nguồn dữ liệu thời gian thực; thứ tự và tốc độ message do lệnh phát theo lô quyết định.",
              "- Đầu ra `streaming_output` chỉ để minh họa, không phải bảng Gold chính thức.",
              "- Phép so với Silver dùng số dòng, số serial và tổng `failure`; consumer không khử trùng khóa (Silver khử trùng, 0 dòng trùng ở dữ liệu này).",
              ""]
    return "\n".join(lines)
