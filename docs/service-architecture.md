# Sơ đồ service MVP

```mermaid
flowchart LR
    source[Backblaze Drive Stats CSV] --> bronze[HDFS Bronze raw CSV]

    subgraph hdfs[HDFS cluster]
        nn[NameNode\nWeb UI :9870]
        dn1[DataNode 1]
        dn2[DataNode 2]
        dn3[DataNode 3]
        nn --- dn1
        nn --- dn2
        nn --- dn3
    end

    bronze --> spark
    subgraph spark[Spark cluster]
        sm[Spark Master\nWeb UI :8080]
        sw1[Worker 1]
        sw2[Worker 2]
        sm --- sw1
        sm --- sw2
        etl[PySpark ETL and Spark SQL]
        ml[Spark MLlib]
    end

    spark --> silver[HDFS Silver Parquet]
    silver --> gold[HDFS Gold analytics features predictions]
    gold --> ui[Streamlit dashboard :8501]
```

## Trách nhiệm service

| Service | Vai trò | Cổng host |
|---|---|---|
| `namenode` | Metadata HDFS, WebHDFS và giao diện kiểm tra block/replica | 9870, 9000 |
| `datanode1` đến `datanode3` | Lưu dữ liệu HDFS, replication factor mặc định là 2 | Không expose |
| `spark-master` | Điều phối Spark applications và hiển thị worker/job | 8080, 7077 |
| `spark-worker1`, `spark-worker2` | Chạy ETL, Spark SQL, feature engineering và MLlib | Không expose |
| `ui-dashboard` | Đọc output Gold đã chuẩn bị và hiển thị dashboard | 8501 |

FastAPI chưa nằm trong MVP Compose. Khi dashboard cần API cho dự đoán theo yêu cầu, thêm service `api` sau khi mô hình, định dạng input và contract endpoint đã ổn định.
