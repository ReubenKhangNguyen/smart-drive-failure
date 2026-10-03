# Dataset split — Gold features, 2026-Q1

Ngay chay: 2026-09-30. Job: `features/label.py` + `features/build_features.py` qua `spark-submit`, doc Silver (30,597,484 dong), ghi `/smart-drive/gold/features`.

Tham so: horizon = 7 ngay; warmup = 30 ngay dau ky (loai truoc 2026-01-31); censoring = 7 ngay cuoi ky (2026-03-31 - 7 = 2026-03-24 la moc ly thuyet, xem luu y ben duoi).

## Ket qua

| Split | So dong | So nhan duong (`fail_within_7_days=1`) | Ty le duong | Khoang ngay |
|---|---|---|---|---|
| train | 10,842,302 | 2,601 | 0.0240% | 2026-01-31 -> 2026-03-03 |
| val | 3,771,986 | 1,227 | 0.0325% | 2026-03-04 -> 2026-03-14 |
| test | 3,435,296 | 963 | 0.0280% | 2026-03-15 -> 2026-03-31 |

So cot dac trung sau ky thuat (`feature_columns()`): **90** (8 cot SMART goc + 8 co `_is_missing` + 8x3 = 72 cot cua so 7/14/30 ngay (`_max`, `_delta`, `_increasing_days`) + 2 cot ma hoa `model_index`/`manufacturer_index`).

`max_train (2026-03-03) < min_val (2026-03-04)`, `max_val (2026-03-14) < min_test (2026-03-15)` — khong chong cheo, dung theo `.claude/rules/ml-leakage.md`.

## Luu y ve khoang ngay test (03-15 -> 03-31, dai hon 03-15->03-24 du kien)

Test van co dong voi ngay > 2026-03-24 vi **mot o co the mang nhan chac chan** ngay ca khi cua so nhan (date, date+7] vuot qua ranh gioi dataset: neu o do da duoc quan sat hong that (`failure=1` trong Silver) trong pham vi con lai cua Q1-2026, nhan `fail_within_7_days` van xac dinh duoc (khong phai suy doan tuong lai). Chi cac dong **khong hong** ma cua so vuot qua ranh gioi moi bi loai (CENSORED). Ket qua: 7 ngay cuoi cung cua test chi con lai cac dong thuoc ve o **da duoc xac nhan hong**, khong con dai dien day du cho quan the o khoe — day la dac diem ky vong cua phuong phap (khong phai loi), nhung can luu y khi doc ket qua danh gia model o Phase 6: metric tren nhung ngay cuoi test co the bi lech nhe do thanh phan mau thay doi.
