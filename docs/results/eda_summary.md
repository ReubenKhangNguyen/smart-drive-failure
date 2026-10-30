# EDA summary — 2026-Q1 (Silver, 30,597,484 dong)

Ngay chay: 2026-09-30. Job: `analytics/smart_analysis.py`, `analytics/failure_analysis.py`, `analytics/drive_model_analysis.py` qua `spark-submit --master spark://spark-master:7077`.

## Tong quan

- Tong dong (drive-days): 30,597,484
- Tong luot hong (`failure=1`): 1,030
- AFR toan bo Q1-2026: 1.23% (annualized tu 90 ngay)
- So o chua tung hong (population healthy): 350,065

## AFR theo hang

| Hang | Drive-days | Failures | AFR |
|---|---|---|---|
| HGST | 2,401,912 | 189 | 2.87% |
| Seagate | 10,068,229 | 406 | 1.47% |
| Toshiba | 10,191,269 | 294 | 1.05% |
| (khong suy ra duoc) | 326,647 | 8 | 0.89% |
| Western Digital | 7,608,887 | 133 | 0.64% |
| Samsung | 540 | 0 | 0.0% (mau qua nho) |

## Top 10 model theo so luong o

| Model | So o | Dung luong TB (byte) | Failures |
|---|---|---|---|
| WDC WUH722222ALE6L4 | 45,638 | 22,000,969,973,760 | 42 |
| TOSHIBA MG08ACA16TA | 40,036 | 16,000,900,661,248 | 102 |
| TOSHIBA MG07ACA14TA | 37,263 | 14,000,519,643,136 | 94 |
| ST16000NM001G | 34,729 | 16,000,900,661,248 | 44 |
| WDC WUH721816ALE6L4 | 26,325 | 16,000,900,661,248 | 56 |
| TOSHIBA MG10ACA20TE | 20,451 | 20,000,588,955,648 | 42 |
| ST12000NM0008 | 18,650 | 12,000,138,625,024 | 129 |
| HGST HUH721212ALE604 | 13,277 | 12,000,138,625,024 | 86 |
| ST8000NM0055 | 13,232 | 8,001,563,222,016 | 39 |
| ST12000NM001G | 13,216 | 12,000,138,625,024 | 33 |

## Tin hieu SMART truoc khi hong (bang chung cho docs/health_rules.md)

Chi tiet day du, ke ca ty le bao dong nham tren nhom o khoe, xem `docs/health_rules.md` muc 3. Tom tat: `smart_5_raw`, `smart_187_raw`, `smart_197_raw`, `smart_198_raw` co tin hieu ro rang (ty le xuat hien o nhom sap hong cao hon nhom khoe 10-24 lan); `smart_9_raw` (gio hoat dong) va `smart_194_raw` (nhiet do) khong phan biet duoc; `smart_199_raw` (loi CRC) khong lien quan den hong o.

## Gioi han

- Chi 1 quy (Q1-2026, 90 ngay) — AFR va nguong the hien dac diem quy nay, chua chac dung cho quy khac.
- `manufacturer` suy tu tien to ten model bang quy tac code, khong phai truong goc tu Backblaze — mot so model la "khong suy ra duoc" (326,647 dong).
