# Baseline luật rules_v1 — Gold health_status, 2026-Q1

Ngay chay: 2026-09-30. Job: `analytics/health_status.py` qua `spark-submit`, ghi HDFS `/smart-drive/gold/health_status`, doc lai tu Silver (`hdfs://namenode:9000/smart-drive/silver/daily`, 30,597,484 dong).

## Phan bo 3 muc

| Muc | So dong | Ty le |
|---|---|---|
| HEALTHY | 28,583,349 | 93.42% |
| WATCH | 1,169,506 | 3.82% |
| CRITICAL | 844,629 | 2.76% |

## Baseline: ty le hong thuc te trong 7 ngay sau, theo tung muc

Loai bo 7 ngay cuoi dataset (right-censoring, khong biet duoc tuong lai). Tong dong con lai sau khi loai: 28,184,333.

| Muc | Dong (sau loai censoring) | Hong trong 7 ngay sau | Ty le hong | Lan cao hon HEALTHY |
|---|---|---|---|---|
| HEALTHY | 26,332,153 | 1,632 | 0.0062% | 1x (nen) |
| WATCH | 1,075,613 | 509 | 0.0473% | ~7.6x |
| CRITICAL | 776,567 | 3,570 | 0.4597% | ~74.2x |

## Nhan xet

- Luat `rules_v1` phan tach ro: ty le hong thuc te tang don dieu theo muc (HEALTHY < WATCH < CRITICAL), CRITICAL cao hon HEALTHY khoang 74 lan.
- Day la **baseline luat** (khong dung ML), dung de so sanh voi model o Phase 6 (mo hinh phai vuot duoc baseline nay tren cung mot metric/tap du lieu).
- Ty le hong tuyet doi con thap ngay ca o nhom CRITICAL (0.46%/7 ngay) vi da so o CRITICAL van tiep tuc hoat dong sau do — dung nhu ky vong cua mot he thong canh bao som (phan lon o duoc phat hien som van chua hong ngay).
