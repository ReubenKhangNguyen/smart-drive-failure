# Đánh giá ngoài thời gian trên Q2-2026 (split `oot`) — 2026-10-04

Mô hình **đóng băng** `v20260930-1` (Logistic Regression, huấn luyện trên Q1, chọn trên validation Q1) chấm điểm toàn bộ Q2-2026 (2026-04-01 → 2026-06-30). Không huấn luyện lại, không chỉnh ngưỡng hay K=100 sau khi thấy Q2. Đánh giá chạy **một lần** (script `scripts/evaluate_oot.py` từ chối chạy lại nếu `artifacts/reports/oot_metrics.json` đã có). Lần chạy đầu bị lỗi hết bộ nhớ trước khi sinh bất kỳ con số nào nên không tính là một lần đánh giá; lần thứ hai cho kết quả dưới đây. Số liệu gốc: `artifacts/reports/oot_metrics.json`.

Baseline luật `rules_v1` xếp hạng bằng `rule_risk_score` trên cùng các dòng. Random Forest không được lưu (không tái hiện được) nên không có mặt ở đây.

## Phân đoạn

Như test Q1, 7 ngày cuối quý (2026-06-24 → 06-30) chỉ còn ổ đã biết chắc hỏng: 272/272 dòng dương, nên top-K đạt 100% với **mọi** phương pháp (cả LR lẫn luật). Số liệu chính là đoạn `normal` (đến 2026-06-23).

| Đoạn | Số dòng | Dòng dương | Tỷ lệ dương |
|---|---|---|---|
| normal (04-01 → 06-23, 84 ngày) | 29.470.040 | 9.343 | 0,0317% |
| tail (06-24 → 06-30) | 272 | 272 | 100% |
| full | 29.470.312 | 9.615 | 0,0326% |

## Kết quả đoạn `normal` (số liệu chính)

| Phương pháp | PR-AUC | ROC-AUC | Recall@100 | Precision@100 |
|---|---|---|---|---|
| **Logistic Regression (đóng băng)** | 0,0277 | 0,8485 | **8,23%** | **9,32%** |
| LR, đồng hạng chia theo margin | — | — | 7,58% | 8,61% |
| Baseline `rules_v1` | — | — | 2,12% | 2,68% |

LR cao hơn luật khoảng 3,9 lần ở recall@100 và 3,5 lần ở precision@100. Chia đồng hạng bằng margin của mô hình (điểm LR bão hòa về 1,0) thấp hơn 0,65 điểm phần trăm recall (8,23% so với 7,58%), lớn hơn khoảng bốn lần độ nhạy đã thấy trên validation Q1 (9,21% so với 9,06%, chênh 0,15). Kết luận LR hơn luật không đổi, nhưng con số tuyệt đối phụ thuộc cách chia đồng hạng.

## Theo tháng (đoạn `normal`, trung bình theo ngày)

| Tháng | Ngày | Dòng dương | LR recall@100 | LR precision@100 | Luật recall@100 | Luật precision@100 |
|---|---|---|---|---|---|---|
| 2026-04 | 30 | 4.255 | 8,53% | 12,13% | 3,67% | 5,23% |
| 2026-05 | 31 | 3.282 | 9,75% | 10,10% | 1,32% | 1,52% |
| 2026-06 (đến 06-23) | 23 | 1.806 | 5,80% | 4,61% | 1,19% | 0,91% |

## So với Q1

| | Q1 validation | Q1 test (`normal`) | Q2 `oot` (`normal`) |
|---|---|---|---|
| PR-AUC | 0,0245 | 0,0204 (toàn test) | 0,0277 |
| ROC-AUC | 0,8545 | 0,8567 (toàn test) | 0,8485 |
| Recall@100 | 9,21% | 5,55% | 8,23% |
| Precision@100 | 10,27% | 3,80% | 9,32% |
| Luật recall@100 | 1,64% | 3,47% | 2,12% |

## Diễn giải và giới hạn

- **Mô hình giữ được khả năng xếp hạng sang quý sau**: ROC-AUC 0,8485 gần Q1 (0,8545 và 0,8567), PR-AUC không giảm. Không có dấu hiệu rò rỉ (không chỉ số nào quá cao, đoạn tail được tách riêng).
- **Recall@100 không so trực tiếp được giữa các quý**: Q2 có khoảng 111 dòng dương mỗi ngày (9.343 / 84), nhiều hơn Q1 test normal (khoảng 68 mỗi ngày). Khi số dương mỗi ngày vượt K=100, recall tối đa bị chặn dưới 100%, còn precision thì không. Precision@100 (9,32%) so sánh công bằng hơn.
- **Xu hướng giảm trong tháng 6** (recall 5,80%, precision 4,61%): chưa kết luận được là do trôi dữ liệu. Chỉ có một quý kiểm chứng, tháng 6 có ít dòng dương hơn và chưa có khoảng tin cậy.
- **Luật `rules_v1` yếu đi rõ rệt theo tháng** (recall 3,67% → 1,32% → 1,19%); ngưỡng của luật được rút ra từ Q1.
- Q2 có 1.522 lượt hỏng so với 1.030 của Q1 (xem `docs/PROGRESS.md`); nguyên nhân chưa được điều tra.
- Con số `full` (recall@100 15,29%, precision@100 16,30%) bị đoạn tail chi phối và **không được công bố đứng một mình**.
- Phân tích mô tả hồi cứu trên một quý kế tiếp; chưa phải bằng chứng cho hiệu quả vận hành thực tế.
