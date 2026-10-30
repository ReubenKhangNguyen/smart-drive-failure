# Độ nhạy của recall@100 trên val theo cách chia hòa — model v20260930-1 (logistic_regression)

Chỉ đọc, chỉ tập val, không đụng test, không ghi Gold. Đây là phân tích độ nhạy, không dùng để chọn lại model.

| Cách chia hòa | Recall@100 | Precision@100 |
|---|---|---|
| serial_number (đã công bố ở Phase 6) | 9.2072% | 10.2727% |
| serial_number (tính lại) | 9.2072% | 10.2727% |
| margin giảm dần, rồi serial | 9.0579% | 10.0909% |

- Số ngày val: 11; số ngày có ổ bị loại khỏi Top-100 chỉ vì chia hòa: 9; tổng số ổ bị loại như vậy: 24; số ổ hòa điểm lớn nhất tại ranh giới Top-100: 11.
- Ngày chấm điểm 2026-03-24: 342,662 ổ, 109 ổ có điểm đúng 1.0; 109 ổ hòa điểm tại ranh giới Top-100, trong đó 9 ổ bị loại chỉ vì chia hòa; 1 ổ trùng cả điểm lẫn margin tại ranh giới.
