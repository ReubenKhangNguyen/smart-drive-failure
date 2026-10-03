# Data quality report - Silver 2026-Q1

Rows in (Bronze after header parse): 30597484
Rows out (Silver): 30597484
Duplicates removed (same serial_number+date): 0
Elapsed seconds: 248.3

Duplicate keys remaining in Silver (should be 0): 0
Serials reappearing after their own failure=1 row: 0
Rows with implausible smart_5_raw < 0: 0

Null rate per SMART column:
- smart_5_raw: 0.0067
- smart_9_raw: 0.0013
- smart_187_raw: 0.6681
- smart_188_raw: 0.6690
- smart_194_raw: 0.0013
- smart_197_raw: 0.0251
- smart_198_raw: 0.0077
- smart_199_raw: 0.0068

Ghi chú: smart_187_raw/smart_188_raw null ~67% vi day la thuoc tinh SMART chi co tren o SAS/enterprise, khong phai loi du lieu.

Silver output size (`hdfs dfs -du -s -h /smart-drive/silver/daily`): 286.4 M thuc te, 572.7 M ke ca replication=2. So voi Bronze CSV 11.2 GB (replication=1) -> nen Parquet giam khoang 39 lan.
`hdfs dfs -count`: 90 thu muc phan vung (date=YYYY-MM-DD), khop 90 ngay Q1-2026.
