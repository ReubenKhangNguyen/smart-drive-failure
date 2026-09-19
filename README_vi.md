# SMART Drive Failure

Đây là bộ khung MVP cho đề tài **Phân tích dữ liệu S.M.A.R.T. và dự đoán nguy cơ hỏng ổ cứng sử dụng Apache Spark**. Hệ thống sẽ lưu Backblaze Drive Stats trên HDFS, xử lý bằng PySpark và Spark SQL, tạo nhãn ổ có nguy cơ hỏng trong bảy ngày tiếp theo, huấn luyện Spark MLlib, rồi đưa các bảng Gold đã chuẩn bị lên Streamlit.

Hiện tại repository chỉ có hạ tầng Docker Compose, cấu hình HDFS/Spark và các file placeholder mô tả trách nhiệm của từng lớp. Không có dữ liệu Backblaze, logic ETL, feature engineering, huấn luyện mô hình hay API. MVP sử dụng 1 NameNode, 3 DataNode, 1 Spark Master, 2 Spark Worker và Streamlit; không gồm FastAPI, Kafka, Neo4j, Airflow hoặc MinIO.

Luồng dự kiến là: Backblaze CSV → HDFS Bronze → PySpark Silver Parquet → Gold features/analytics/predictions → Spark MLlib → Streamlit. Xem `docs/quick-start.md` để khởi động hạ tầng và `docs/service-architecture.md` để xem các service.
