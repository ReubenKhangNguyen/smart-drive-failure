"""Static content of the page "Quy trình 9 bước": for each course step what it is for, the theory behind it, what to
prepare, what happens inside, and its input and output. The live part (status, log, measurements) comes from the
evidence files of the demo run (artifacts/demo/steps/*.json).

The dashboard container only has ui_dashboard/ and artifacts/, so it cannot import pipeline/demo_spec.py; this list
repeats the task ids, and tests/test_walkthrough.py checks they stay equal to the real DAG.
"""
from __future__ import annotations

from typing import Any, Dict, List

AIRFLOW_URL = "http://localhost:8081"
HDFS_URL = "http://localhost:9870/explorer.html#/smart-drive-demo"
SPARK_URL = "http://localhost:8080"
DAG_ID = "smart_drive_demo"

# (task id, label, short title) in DAG order: the page lists one row per task in the timeline.
TASKS = [
    ("prepare_hdfs", "1", "Chuẩn bị HDFS"),
    ("upload_data", "2", "Upload dữ liệu lên HDFS"),
    ("read_spark", "3", "Đọc bằng Spark"),
    ("clean_data", "4", "Làm sạch dữ liệu"),
    ("spark_sql", "5", "Spark SQL"),
    ("kmeans", "6a", "Phân cụm K-Means"),
    ("build_features", "6b", "Gán nhãn và tạo đặc trưng"),
    ("train_model", "6c", "Huấn luyện và lưu mô hình"),
    ("score_days", "6d", "Chấm điểm rủi ro"),
    ("save_results", "7", "Lưu và xuất kết quả"),
    ("kafka_produce", "8a", "Kafka: phát dữ liệu"),
    ("kafka_consume", "8b", "Kafka: Spark nhận và xử lý"),
    ("finish_summary", "9", "Tổng kết luồng (Airflow DAG)"),
]

STEPS: List[Dict[str, Any]] = [
    {
        "step": 1, "title": "Chuẩn bị HDFS", "tasks": ["prepare_hdfs"],
        "goal": "Dựng chỗ chứa dữ liệu trên HDFS theo ba lớp Bronze, Silver, Gold trước khi nạp bất cứ thứ gì.",
        "theory": [
            "HDFS chia mỗi file thành các khối (block, ở đây 128 MB) và nhân bản chúng trên nhiều DataNode; NameNode chỉ giữ siêu dữ liệu (file nào gồm khối nào, khối nằm ở đâu).",
            "Nhân bản giúp chịu lỗi khi một DataNode hỏng và cho Spark đọc dữ liệu ở gần nơi nó nằm.",
            "Kiến trúc Medallion tổ chức dữ liệu thành các lớp chất lượng tăng dần: Bronze (thô, bất biến), Silver (đã làm sạch), Gold (đã tổng hợp cho từng mục đích).",
        ],
        "prepare": ["Docker Desktop đang chạy, được cấp ít nhất 8 GB RAM", "Cụm HDFS đã khởi động (docker compose up -d): NameNode và 3 DataNode khỏe", "Airflow đã chạy để bấm Trigger DAG (profile orchestration)"],
        "inside": ["Hỏi NameNode (JMX) xem còn bao nhiêu DataNode sống và dung lượng", "Tạo thư mục bằng WebHDFS (MKDIRS) chỉ trong vùng /smart-drive-demo, không đụng dữ liệu thật", "Liệt kê cây thư mục vừa tạo để kiểm tra"],
        "input": "Cụm HDFS còn trống", "output": "Thư mục /smart-drive-demo/{bronze, silver, gold, models}",
    },
    {
        "step": 2, "title": "Upload dữ liệu", "tasks": ["upload_data"],
        "goal": "Đưa các file CSV Backblaze từ đĩa máy vào HDFS Bronze, nguyên bản, và kiểm tra đã đủ.",
        "theory": [
            "Lớp thô phải bất biến (không sửa dữ liệu gốc) và nạp phải chạy lại được (idempotent): chạy hai lần không được làm hỏng hay nhân đôi dữ liệu.",
            "Phải kiểm tra trước khi tin dữ liệu: đủ ngày chưa, mỗi file có đủ cột bắt buộc chưa.",
        ],
        "prepare": ["Thư mục CSV nằm trên máy và được mount vào container Airflow (docker-compose.yml)", "Bước 1 đã tạo thư mục Bronze"],
        "inside": ["Kiểm tra nguồn: đủ 28 ngày, mỗi file có đủ các cột bắt buộc", "Nạp từng file qua WebHDFS (replication 1: file gốc luôn tải lại được); file đã có cùng kích thước thì bỏ qua", "Kiểm kê lại trên HDFS: đủ ngày, và dung lượng khớp từng byte với đĩa máy"],
        "input": "28 file CSV trên đĩa máy (khoảng 3,7 GB, 197 cột)", "output": "28 file CSV nguyên bản trong HDFS Bronze",
    },
    {
        "step": 3, "title": "Đọc bằng Spark", "tasks": ["read_spark"],
        "goal": "Cho Spark đọc CSV trực tiếp từ HDFS và chia việc ra nhiều partition để xử lý song song.",
        "theory": [
            "Spark chia dữ liệu thành các partition, mỗi partition do một core xử lý; nhiều worker chạy song song.",
            "Phép biến đổi (select, filter...) là lười: chỉ ghi kế hoạch; chỉ khi có phép hành động (count, write) Spark mới chạy thật.",
            "inferSchema bắt Spark quét dữ liệu thêm một lượt để đoán kiểu, vừa chậm vừa dễ đoán sai, nên tắt và ép kiểu tường minh ở bước sau.",
        ],
        "prepare": ["Spark Master và 2 worker đang chạy (http://localhost:8080)", "Bronze đã có CSV (bước 2)"],
        "inside": ["spark.read với header=true, không inferSchema: mọi cột vào dưới dạng chuỗi", "In lược đồ, số partition, đếm số dòng (lúc này Spark mới đọc thật)", "Đo thử inferSchema trên một file để thấy chi phí của nó"],
        "input": "28 file CSV trong Bronze", "output": "Một DataFrame 197 cột, toàn chuỗi, chưa có kiểu",
    },
    {
        "step": 4, "title": "Làm sạch dữ liệu", "tasks": ["clean_data"],
        "goal": "Biến CSV thô thành bảng Silver đáng tin: đúng kiểu, không trùng, chuẩn hóa, lưu dạng Parquet.",
        "theory": [
            "Làm sạch gồm ép kiểu, loại giá trị bất hợp lý, loại bản ghi trùng và chuẩn hóa.",
            "Parquet là định dạng lưu theo cột có nén: truy vấn chỉ đọc cột cần thiết, file nhỏ hơn nhiều so với CSV.",
            "Giá trị khuyết (null) được giữ nguyên, không điền 0: thiếu một thuộc tính khác với bằng 0 (ví dụ chỉ ổ SAS báo smart_187).",
        ],
        "prepare": ["DataFrame thô của bước 3"],
        "inside": ["Chọn 13 cột cần dùng trong 197 cột và ép kiểu tường minh", "Dung lượng âm thành null, suy ra hãng từ tiền tố tên model", "Loại dòng trùng khóa (serial_number, date) bằng hàm cửa sổ", "Ghi Parquet phân vùng theo ngày (ghi đè phân vùng động: chạy lại không xóa ngày khác)", "Kiểm tra chất lượng: trùng khóa, giá trị âm, tỷ lệ null, so dung lượng với Bronze"],
        "input": "DataFrame thô 197 cột toàn chuỗi", "output": "Silver: Parquet, khóa (serial_number, date)",
    },
    {
        "step": 5, "title": "Spark SQL", "tasks": ["spark_sql"],
        "goal": "Trả lời câu hỏi nghiệp vụ bằng truy vấn SQL trên Silver: hãng nào hỏng nhiều, chỉ số nào báo trước hỏng, tình trạng từng ổ.",
        "theory": [
            "Spark SQL đăng ký DataFrame thành view tạm rồi truy vấn bằng SQL; bộ tối ưu truy vấn tự chọn kế hoạch chạy hiệu quả.",
            "AFR (tỷ lệ hỏng hằng năm) = số lượt hỏng chia cho (số ổ-ngày chia 365), quy mọi nhóm về cùng một cơ sở thời gian để so sánh.",
            "Luật rules_v1 xếp mỗi ổ-ngày vào HEALTHY, WATCH hoặc CRITICAL từ bốn chỉ số lõi (smart_5, 187, 197, 198) và được kiểm bằng tỷ lệ hỏng thật ở 7 ngày sau.",
        ],
        "prepare": ["Silver đã có (bước 4)"],
        "inside": ["spark.sql tính AFR theo hãng và model, phân phối chỉ số, tỷ lệ ổ hỏng có tín hiệu trước khi hỏng", "Áp luật rules_v1 cho mọi ổ-ngày và đo tỷ lệ hỏng thật theo từng mức", "Ghi các bảng Gold nhỏ"],
        "input": "Silver", "output": "Gold phân tích (AFR, tín hiệu, phân phối) và health_status",
    },
    {
        "step": 6, "title": "Phân tích nâng cao: K-Means và học máy", "tasks": ["kmeans", "build_features", "train_model", "score_days"],
        "goal": "Nhóm ổ theo hành vi (K-Means) và dự đoán ổ nào có nguy cơ hỏng trong 7 ngày tới.",
        "theory": [
            "K-Means chia các điểm thành K nhóm sao cho khoảng cách tới tâm cụm nhỏ nhất; chọn K bằng hệ số silhouette (đo mức tách biệt). Ở đây chỉ để mô tả, mã cụm không làm đặc trưng của mô hình.",
            "Nhãn dương cực hiếm nên không dùng accuracy: dùng recall@K (trong tất cả ổ sẽ hỏng, bao nhiêu nằm trong K ổ có điểm cao nhất mỗi ngày) và huấn luyện có trọng số lớp.",
            "Chống rò rỉ tương lai: chia tập theo thời gian (không ngẫu nhiên), đặc trưng ngày t chỉ dùng dữ liệu đến ngày t, loại 7 ngày cuối vì chưa biết kết quả (right-censoring).",
            "Điểm rủi ro là điểm xếp hạng (có trọng số lớp), không phải xác suất hỏng.",
        ],
        "prepare": ["Silver và health_status (bước 4, 5)"],
        "inside": ["6a. K-Means trên bốn chỉ số lõi, thử nhiều K, chọn theo silhouette; ổ có cả bốn chỉ số bằng 0 tách riêng", "6b. Gán nhãn fail_within_7_days, tạo 90 đặc trưng cửa sổ, chia train / validation / test theo thời gian", "6c. Huấn luyện Logistic Regression có trọng số lớp, đo recall@K trên validation so với luật, lưu mô hình", "6d. Tải mô hình, chấm điểm, xếp hạng Top-K mỗi ngày"],
        "input": "Silver, health_status", "output": "Bảng cụm, Gold features, mô hình đã lưu, điểm rủi ro và Top-K theo ngày",
    },
    {
        "step": 7, "title": "Lưu kết quả", "tasks": ["save_results"],
        "goal": "Lưu kết quả đúng nơi và đúng dạng để người dùng xem: Gold trên HDFS và các bảng nhỏ cho bảng điều khiển.",
        "theory": [
            "Bảng cho giao diện nên nhỏ để đọc nhanh mà không cần chạy lại tính toán; bảng điều khiển chỉ đọc chúng, không đọc HDFS và không cần Spark chạy.",
            "Mỗi bảng có hợp đồng dữ liệu (cột, kiểu, khóa) để bên đọc và bên ghi không lệch nhau.",
        ],
        "prepare": ["Các bảng Gold của bước 5 và 6, và mô hình đã lưu"],
        "inside": ["Gom Gold thành các bảng nhỏ (Parquet một file) cho dashboard", "Đo lại dung lượng và số file của từng lớp trên HDFS"],
        "input": "Các bảng Gold, mô hình", "output": "Bảng xuất cho dashboard và các lớp dữ liệu trên HDFS",
    },
    {
        "step": 8, "title": "Kafka ingest", "tasks": ["kafka_produce", "kafka_consume"],
        "goal": "Mô phỏng dữ liệu đổ về liên tục: producer gửi vào Kafka, Spark Structured Streaming nhận và làm sạch.",
        "theory": [
            "Kafka là kênh trung gian theo mô hình producer, topic, consumer: lưu message theo thứ tự (mỗi message có offset), tách nơi sinh dữ liệu khỏi nơi dùng.",
            "Kafka không đẩy dữ liệu: consumer chủ động hỏi. Spark Structured Streaming lặp lại micro-batch: hỏi offset mới nhất, đọc đoạn chưa xử lý, xử lý, ghi, rồi ghi offset vào checkpoint nên làm tiếp đúng chỗ và không mất dữ liệu.",
            "Đây là lớp minh họa: batch (bước 2 đến 7) vẫn là đường chính, và luồng này mới làm sạch, chưa chấm điểm bằng mô hình.",
        ],
        "prepare": ["Kafka và ZooKeeper phải chạy trước: docker compose --profile streaming up -d zookeeper kafka", "Container Airflow ra được internet (tải kafka-python và gói spark-sql-kafka một lần)"],
        "inside": ["8a. Producer gửi dữ liệu một ngày vào topic mới, lô 2.000 message mỗi 0,2 giây, chỉ 13 cột (~314 byte/message)", "8b. Spark đọc topic theo micro-batch (tối đa 50.000 message), làm sạch bằng chính hàm của bước 4, ghi Parquet", "Đối chiếu với Silver cùng ngày: số dòng, số serial, tổng failure phải khớp"],
        "input": "Dữ liệu CSV của một ngày", "output": "Parquet streaming, khớp Silver cùng ngày",
    },
    {
        "step": 9, "title": "Airflow DAG", "tasks": ["finish_summary"],
        "goal": "Điều phối cả chuỗi: mỗi bước là một tác vụ, chạy đúng thứ tự, dừng khi có bước lỗi, cho xem trạng thái từng bước.",
        "theory": [
            "DAG là đồ thị có hướng không chu trình: nút là tác vụ, mũi tên là phụ thuộc 'xong bước này mới chạy bước sau'.",
            "DAG không chứa logic xử lý, chỉ gọi lại các script có sẵn; vì vậy sửa pipeline không phải sửa DAG.",
            "Các chốt an toàn: mỗi lần chỉ một tác vụ Spark (cụm nhỏ), không tự thử lại khi lỗi, mỗi tác vụ có giới hạn thời gian.",
        ],
        "prepare": ["Airflow đang chạy (http://localhost:8081, cần đăng nhập)"],
        "inside": ["Bấm Trigger DAG smart_drive_demo; đồ thị đổi màu từng bước", "Tác vụ cuối tổng kết trạng thái và thời gian của mọi bước"],
        "input": "Các tác vụ của bước 1 đến 8", "output": "Một lần chạy trọn vẹn, có trạng thái và log từng bước",
    },
]

CHAIN = [
    ("CSV trên đĩa", "Bronze (HDFS)", "2. upload"),
    ("Bronze (HDFS)", "DataFrame thô", "3. Spark đọc"),
    ("DataFrame thô", "Silver (Parquet)", "4. làm sạch"),
    ("Silver (Parquet)", "Gold phân tích", "5. Spark SQL"),
    ("Silver (Parquet)", "Gold features và mô hình", "6. K-Means, học máy"),
    ("Gold phân tích", "Bảng cho dashboard", "7. lưu kết quả"),
    ("Gold features và mô hình", "Bảng cho dashboard", "7. lưu kết quả"),
    ("CSV trên đĩa", "Kafka topic", "8a. producer"),
    ("Kafka topic", "Parquet streaming", "8b. Spark Streaming"),
]
