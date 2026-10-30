# Demo streaming Kafka (bước 8 của giảng viên) — 20261003075625

Lớp trình diễn bổ sung: **batch pipeline (Bronze → Silver → Gold) vẫn là nguồn chính cho phân tích và huấn luyện**; Kafka chỉ mô phỏng dữ liệu SMART đổ về hằng ngày và chấm làm sạch gần thời gian thực. Chạy đúng 1 ngày dữ liệu (2026-01-01), message chỉ gồm 13 cột consumer dùng (~314 byte, thay vì ~4,885 byte nếu gửi cả 197 cột), phát theo lô 2,000 message mỗi 0.2 giây.

## Kiểm chứng

| Kiểm tra | Giá trị | Kết quả |
|---|---|---|
| Producer gửi / broker xác nhận / lỗi | 338,760 / 338,760 / 0 | ĐẠT |
| Consumer đọc từ Kafka | 338,760 dòng | ĐẠT |
| Dòng Parquet trong `streaming_output/20261003075625` (đã qua `cast_and_clean_columns`) | 338,760 | ĐẠT |
| So với Silver 2026-01-01: dòng | 338,760 so với 338,760 | ĐẠT |
| So với Silver: số serial khác nhau | 338,760 so với 338,760 | ĐẠT |
| So với Silver: tổng `failure` | 1 so với 1 | ĐẠT |

## Micro-batch của Spark Structured Streaming

372 micro-batch có dữ liệu, tổng 338,760 dòng; mỗi batch trung bình 911 dòng (nhỏ nhất 48, lớn nhất 23,355), consumer bắt kịp tốc độ producer thay vì đợi cả file.

| Batch | Số dòng | Thời gian xử lý (ms) | Dòng/giây |
|---|---|---|---|
| 0 | 2,900 | 8285 | 350 |
| 1 | 23,355 | 1460 | 15,997 |
| 2 | 5,173 | 545 | 9,492 |
| 3 | 1,898 | 408 | 4,652 |
| 4 | 1,133 | 464 | 2,442 |
| … | … (362 batch ở giữa) | … | … |
| 367 | 408 | 174 | 2,345 |
| 368 | 970 | 170 | 5,673 |
| 369 | 622 | 225 | 2,764 |
| 370 | 624 | 1077 | 579 |
| 371 | 136 | 206 | 660 |

Consumer dừng vì: `idle` (không có dữ liệu mới trong khoảng chờ, hoặc hết thời gian tối đa).

## Thời gian

- Producer: 97.9 giây; consumer (từ lúc khởi động đến khi dừng): 141.6 giây.
- Topic `smart-events-20261003075625`, `maxOffsetsPerTrigger` = 50,000, executor mặc định (không đổi cỡ).

## RAM (docker stats)

- Trước khi bật Kafka/ZooKeeper: 2,062 MiB (tổng các container).
- Sau khi Kafka/ZooKeeper chạy, chưa có tải: 2,460 MiB.
- **Đỉnh trong lúc chạy demo: 4,865 MiB (4.75 GiB)** trên 23 mẫu; giới hạn Docker 7,533 MiB.

| Container | Đỉnh (MiB) |
|---|---|
| smart-drive-spark-master | 1,149 |
| smart-drive-spark-worker1 | 858 |
| smart-drive-spark-worker2 | 718 |
| smart-drive-namenode | 554 |
| smart-drive-kafka | 400 |
| smart-drive-datanode3 | 379 |
| smart-drive-datanode2 | 374 |
| smart-drive-datanode1 | 367 |
| smart-drive-zookeeper | 77 |

## Giới hạn

- Đây là mô phỏng luồng trên một ngày dữ liệu có sẵn, không phải nguồn dữ liệu thời gian thực; thứ tự và tốc độ message do lệnh phát theo lô quyết định.
- Đầu ra `streaming_output` chỉ để minh họa, không phải bảng Gold chính thức.
- Phép so với Silver dùng số dòng, số serial và tổng `failure`; consumer không khử trùng khóa (Silver khử trùng, 0 dòng trùng ở dữ liệu này).
