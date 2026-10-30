# Phân cụm K-Means ổ cứng — 2026-10-03

Giới hạn: Phân tích mô tả hồi cứu, mẫu lệch ngày (ổ khỏe lấy ngày quan sát cuối quý, ổ hỏng lấy ngày liền trước ngày hỏng). Không so sánh trực tiếp với recall@K của LR; `failed_rate` không phải xác suất hỏng. Cluster id không được dùng làm đặc trưng của model dự đoán.

- K được chọn (silhouette lớn nhất trong khoảng thử): **3**
- Số ổ trong mẫu: 350,935 (ổ hỏng 870 / tổng ổ hỏng 1,030; **160 ổ hỏng bị loại** vì không có dòng ngày liền trước ngày hỏng)
- Nhóm `zero_signal` (cả 4 chỉ số = 0, không đưa vào K-Means): 326,241 ổ, trong đó **239 ổ hỏng** (giới hạn của cách tiếp cận dựa trên 4 chỉ số này, dùng cho phần phân tích lỗi)

| K | Silhouette | WSSSE | Chọn |
|---|---|---|---|
| 3 | 0.6870 | 48490.0 | x |
| 4 | 0.6402 | 40976.3 |  |
| 5 | 0.6266 | 36207.7 |  |
| 6 | 0.5345 | 29581.1 |  |
| 7 | 0.5474 | 25017.0 |  |
| 8 | 0.4523 | 22912.2 |  |

| Cụm | Loại | Số ổ | Khỏe | Hỏng | failed_rate | smart_5 TB | smart_187 TB | smart_197 TB | smart_198 TB | smart_187 null |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | zero_signal | 326,241 | 326,002 | 239 | 0.0007 | 0.0 | 0.0 | 0.0 | 0.0 | 68.2% |
| 1 | kmeans | 18,828 | 18,621 | 207 | 0.0110 | 144.2 | 1.2 | 1.1 | 0.3 | 51.7% |
| 2 | kmeans | 1,846 | 1,722 | 124 | 0.0672 | 12139.2 | 608.5 | 547.7 | 548.4 | 0.0% |
| 3 | kmeans | 4,020 | 3,720 | 300 | 0.0746 | 96.0 | 6.3 | 988.1 | 826.3 | 67.9% |
