# Đánh giá mô hình dự đoán hỏng ổ cứng (fail_within_7_days)

Dữ liệu: 2026-Q1 (90 ngày). Train 2026-01-31→03-03 (10,842,302 dòng), val 2026-03-04→03-14 (3,771,986 dòng), test 2026-03-15→03-31 (3,435,296 dòng). K=100 (theo `docs/charter.md`). Model chính thức: **Logistic Regression**, chốt trên val trước khi chạy test (xem `docs/decisions.md`, mục ghi ngày 2026-09-30). Test chạy đúng **một lần duy nhất**, 2026-09-30 16:15–16:36.

## 1. Kết quả trên tập validation (dùng để chọn model)

| Phương pháp | PR-AUC | ROC-AUC | Recall@100 | Precision@100 |
|---|---|---|---|---|
| **Logistic Regression** | 0.0245 | 0.8545 | **9.21%** | **10.27%** |
| Random Forest | 0.0349 | 0.9149 | 6.92% | 7.82% |
| Baseline `rules_v1` | — | — | 1.64% | 1.73% |

LR được chọn vì thắng RF ở đúng metric ưu tiên trong charter ("Recall ở K cố định"), dù RF có PR-AUC/ROC-AUC cao hơn. Cả hai đều vượt baseline luật rõ rệt (LR gấp 5.6 lần, RF gấp 4.2 lần recall@100).

## 2. Kết quả trên toàn bộ tập test

| Phương pháp | PR-AUC | ROC-AUC | Recall@100 | Precision@100 |
|---|---|---|---|---|
| **Logistic Regression** | 0.0204 | 0.8567 | 44.44% | 43.41% |
| Random Forest | 0.0148 | 0.8863 | 43.15% | 42.53% |
| Baseline `rules_v1` | — | — | 43.22% | 42.59% |

PR-AUC và ROC-AUC ổn định so với val (LR: 0.0245→0.0204, 0.8545→0.8567) — không có dấu hiệu leakage theo `.claude/rules/ml-leakage.md` (không có chỉ số nào > 0.9, không có recall gần 100% trên **toàn thể**, val không tốt hơn test một cách bất thường).

Tuy nhiên **recall@100/precision@100 trên toàn bộ test tăng vọt bất thường** so với val (44% so với 9%) trong khi PR-AUC/ROC-AUC gần như không đổi. Mục 3 giải thích nguyên nhân bằng số liệu.

## 3. Vì sao recall@100/precision@100 trên test cao bất thường — không phải vì model giỏi hơn

Tách test thành 2 đoạn theo ngày và tính lại recall/precision@100 cho cả 3 phương pháp:

| Đoạn | Số dòng | Số dòng nhãn dương | Tỷ lệ dương |
|---|---|---|---|
| **normal** (2026-03-15 → 03-24) | 3,435,008 | 675 | 0.0197% |
| **tail** (2026-03-25 → 03-31) | 288 | **288 (100%)** | 100% |

| Phương pháp | Recall@100 (normal) | Precision@100 (normal) | Recall@100 (tail) | Precision@100 (tail) |
|---|---|---|---|---|
| **Logistic Regression** | **5.55%** | **3.80%** | 100% | 100% |
| Random Forest | 3.36% | 2.30% | 100% | 100% |
| Baseline `rules_v1` | 3.47% | 2.40% | 100% | 100% |

**Nguyên nhân (phương pháp luận, không phải điểm mạnh mô hình):** `features/label.py` gán nhãn chắc chắn cho một ổ nếu nó đã được quan sát hỏng thật (`failure=1`) trong dữ liệu, kể cả khi cửa sổ (date, date+7] vượt ranh giới cuối dataset — đây là lựa chọn đúng đắn (không suy đoán tương lai với ổ chưa biết kết quả), nhưng hệ quả là: với các ổ **không** hỏng, cửa sổ vượt ranh giới sẽ bị loại (CENSORED, đúng luật right-censoring). Kết quả: 7 ngày cuối dataset (nằm trong test) chỉ còn lại dòng của những ổ **đã biết chắc sẽ hỏng** — toàn bộ 288 dòng còn lại trong 7 ngày đó đều là nhãn dương. Việc chọn Top-100 trong một ngày mà *tất cả* các dòng đều dương sẽ luôn đạt recall=100%/precision=100% một cách tầm thường, không phản ánh năng lực phân biệt của bất kỳ phương pháp nào — bằng chứng là **cả 3 phương pháp hoàn toàn khác nhau (2 model ML + 1 luật ngưỡng đơn giản) đều đạt chính xác 100% ở đoạn này**.

Thêm vào đó, `macro_average_at_k` tính trung bình **theo ngày**, không theo trọng số số dòng: 7/17 ngày của test (41%) rơi vào đoạn "tail" này, nên dù đoạn đó chỉ chiếm 0.008% tổng số dòng, nó vẫn đóng góp 41% trọng số vào con số trung bình cuối cùng — đẩy recall@100 "toàn bộ test" từ ~5-6% lên ~43-44%.

**Kết luận: con số đáng tin cậy hơn để đánh giá model là đoạn "normal" (99.99% dữ liệu test, không bị ảnh hưởng bởi đặc điểm nhân tạo cuối kỳ).** Trên đoạn này, **LR vẫn thắng rõ cả RF và baseline luật** (~1.6× recall@100, ~1.6-1.65× precision@100 so với cả hai) — nhất quán với kết luận đã chốt trên val, không bị đổi bởi test.

## 4. Feature importance (Random Forest, top 15)

| Hạng | Đặc trưng | Importance |
|---|---|---|
| 1 | `smart_197_raw_max_7d` | 0.1117 |
| 2 | `smart_5_raw_max_30d` | 0.1052 |
| 3 | `smart_197_raw_max_14d` | 0.0741 |
| 4 | `smart_5_raw_max_14d` | 0.0613 |
| 5 | `smart_197_raw_max_30d` | 0.0601 |
| 6 | `smart_198_raw_max_30d` | 0.0592 |
| 7 | `smart_5_raw_max_7d` | 0.0514 |
| 8 | `smart_197_raw` (giá trị hiện tại) | 0.0507 |
| 9 | `smart_197_raw_increasing_days_30d` | 0.0496 |
| 10 | `smart_198_raw_max_7d` | 0.0425 |
| 11 | `smart_5_raw` (giá trị hiện tại) | 0.0379 |
| 12 | `model_index` | 0.0320 |
| 13 | `smart_198_raw` (giá trị hiện tại) | 0.0162 |
| 14 | `smart_187_raw_max_14d` | 0.0152 |
| 15 | `smart_187_raw_max_7d` | 0.0150 |

Không có đặc trưng đơn lẻ nào áp đảo (cao nhất 0.112, xa dưới ngưỡng đáng ngờ) — khớp với 4 cột `smart_5/187/197/198_raw` đã xác định trong `docs/health_rules.md` từ EDA Phase 4, cộng thêm cửa sổ 7/14/30 ngày góp phần đáng kể, xác nhận feature engineering ở Phase 5 có đóng góp thật.

## 5. Phân tích lỗi (sơ bộ)

- Model phụ thuộc nhiều vào `smart_197_raw` (current pending sectors) và `smart_5_raw` (reallocated sectors) ở các cửa sổ dài (14-30 ngày) hơn là giá trị tức thời — gợi ý xu hướng tích lũy quan trọng hơn một lần đọc đơn lẻ.
- Baseline luật (chỉ dùng 4 cột, không cửa sổ) vẫn khá cạnh tranh ở đoạn normal (3.47% so với LR 5.55%) — cho thấy phần lớn tín hiệu nằm ở chính 4 cột SMART đã chọn trong Phase 4, phần cửa sổ hóa (Phase 5) và mô hình hóa (Phase 6) cải thiện thêm nhưng không đột phá.
- Chưa phân tích chi tiết các ca bỏ sót cụ thể (which drives) do giới hạn thời gian — để trong hướng phát triển.

### 5.1. Ổ hỏng không có tín hiệu ở bốn chỉ số SMART

Trong 1,030 ổ hỏng của 2026-Q1, một phần đáng kể không phát ra tín hiệu nào ở bốn chỉ số mà luật `rules_v1` và phân cụm K-Means dựa vào (`smart_5`, `smart_187`, `smart_197`, `smart_198`). Một ổ hỏng được coi là "có tín hiệu" nếu ít nhất một trong bốn chỉ số lớn hơn 0 ít nhất một lần trong cửa sổ `[ngày hỏng − d, ngày hỏng − 1]`; giá trị null được coi là không có tín hiệu.

| Cửa sổ trước ngày hỏng | Ổ hỏng có dòng dữ liệu trong cửa sổ (mẫu số) | Có tín hiệu | Không tín hiệu | Tỷ lệ không tín hiệu |
|---|---|---|---|---|
| 1 ngày (ngày liền trước) | 870 | 631 | 239 | 27.5% |
| 7 ngày | 923 | 679 | 244 | 26.4% |
| 30 ngày | 1,003 | 762 | 241 | 24.0% |

**Mẫu số thay đổi theo cửa sổ** (870 / 923 / 1,003): cửa sổ càng dài thì càng có thêm ổ hỏng có ít nhất một dòng dữ liệu trong đó (160, 107 và 27 ổ hỏng lần lượt không có dòng nào trong cửa sổ 1, 7 và 30 ngày, nên bị loại khỏi mẫu số). Vì vậy ba tỷ lệ trên không so sánh trực tiếp từng ổ giữa các hàng. Dù vậy kết luận không đổi: nhìn lùi xa hơn chỉ lấy lại thêm vài điểm phần trăm, và khoảng một phần tư số ổ hỏng không có giá trị dương nào ở bốn chỉ số này trong cả tháng trước đó, nên luật `rules_v1` và phân cụm dựa trên bốn chỉ số này không thể phát hiện chúng.

Mô hình Logistic Regression dùng thêm các thuộc tính khác (`smart_9`, `smart_188`, `smart_194`, `smart_199`) và đặc trưng xu hướng 7/14/30 ngày, nhưng mức độ nó bắt được nhóm ổ này chưa được kiểm tra. Các con số mang tính mô tả hồi cứu trên một quý dữ liệu; ổ hỏng ở đầu quý có cửa sổ 30 ngày bị cắt do thiếu lịch sử.

Đối chiếu và nguồn: 239/870 khớp với nhóm `zero_signal` của `kmeans_clusters` (239 ổ hỏng) và với 870 ổ hỏng trong mẫu K-Means. Bảng `pre_failure_signal` chỉ cho tỷ lệ từng chỉ số riêng lẻ, không cho phép hợp theo từng ổ, nên phần hợp được tính trực tiếp từ Silver bằng `spark.sql` theo cùng định nghĩa cửa sổ (job Spark chỉ đọc, ngày 2026-10-02, không ghi vào repo).

Một giới hạn liên quan của recall@100: điểm rủi ro của LR bão hòa (109 ổ có điểm đúng 1.0 trong ngày chấm điểm 2026-03-24), nên recall@100 phụ thuộc cách chia hòa tại ranh giới Top-100. Trên tập validation, chia hòa bằng serial (cách công bố ở trên) cho 9.2072% và chia hòa bằng margin của mô hình cho 9.0579% (precision@100: 10.2727% so với 10.0909%). Chênh lệch này là độ nhạy, không đổi kết luận chọn model.

## 6. Giới hạn

- Chỉ 1 quý dữ liệu (2026-Q1, 90 ngày); ngưỡng `health_rules.md` và mô hình đều học/tính từ chính quý này — có thể không tổng quát cho quý khác.
- Right-censoring 7 ngày cuối dataset làm đoạn cuối test có đặc điểm nhân tạo (mục 3) — số liệu "toàn bộ test" nên luôn đi kèm số liệu tách đoạn khi trình bày, tránh gây hiểu lầm.
- `manufacturer` suy từ tiền tố tên model bằng quy tắc code, không phải trường gốc từ Backblaze.
- Chưa benchmark Big Data (Phase 9) — thời gian chạy job ghi trong `artifacts/reports/*.md` nhưng chưa so sánh CSV/Parquet hay số worker.
- RF dùng `maxBins=256`, `numTrees=30`, `maxDepth=8` — chưa tinh chỉnh sâu do giới hạn tài nguyên cụm (2 worker × 2GB); PR-AUC/ROC-AUC của RF có thể cải thiện thêm với nhiều cây hơn.
