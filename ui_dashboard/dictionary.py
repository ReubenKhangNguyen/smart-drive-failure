"""Static content of the page "Dữ liệu và thuộc tính": what each column of the dataset means and what it says about a drive.
SMART figures come from docs/health_rules.md (2026-Q1, Silver), so the page and the report quote the same numbers."""
from __future__ import annotations

BASE_COLUMNS = [
    ("date", "Ngày ghi nhận", "Mỗi ngày Backblaze chụp trạng thái của mọi ổ đang chạy.", "Khóa thời gian; dùng chia train, validation, test theo thời gian."),
    ("serial_number", "Số serial của ổ", "Định danh duy nhất một ổ vật lý. Cùng serial theo dõi được ổ đó qua nhiều ngày.", "Khóa cùng với date (một dòng là một ổ trong một ngày). Không dùng làm đặc trưng vì sẽ học thuộc từng ổ."),
    ("model", "Mã sản phẩm của ổ", "Ví dụ ST12000NM0008 (Seagate), TOSHIBA MG08ACA16TA. Mỗi dòng ổ có độ tin cậy khác nhau.", "Suy ra hãng (manufacturer); là đặc trưng phân loại và dùng để tính AFR theo model."),
    ("capacity_bytes", "Dung lượng", "Số byte của ổ (ví dụ 16 TB = 16.000.900.661.248 byte).", "Giá trị âm bị đổi thành null khi làm sạch; dùng mô tả cấu thành đội ổ."),
    ("failure", "Cờ hỏng", "Bằng 1 vào đúng ngày ổ hỏng và bị loại khỏi đội, bằng 0 các ngày còn lại.", "Cơ sở tạo nhãn dự đoán. Chỉ có khoảng 0,003% số dòng bằng 1 (1.030 trên 30,6 triệu dòng Q1)."),
]

DERIVED_COLUMNS = [
    ("manufacturer", "Hãng sản xuất", "Suy ra từ tiền tố tên model bằng quy tắc trong mã (Backblaze không có cột này)."),
    ("fail_within_7_days", "Nhãn cần dự đoán", "1 nếu ổ hỏng trong 7 ngày tiếp theo; loại 7 ngày cuối dataset vì chưa biết kết quả."),
    ("health_level", "Mức tình trạng của luật", "HEALTHY, WATCH hoặc CRITICAL theo luật rules_v1 từ bốn chỉ số lõi, kèm lý do."),
    ("risk_score", "Điểm rủi ro của mô hình", "Điểm xếp hạng của Logistic Regression có trọng số lớp, không phải xác suất hỏng."),
]

# attribute, name, what it counts, what it says, role in the project, healthy %>0, pre-failure %>0 within 7 days
SMART = [
    ("smart_5_raw", "Reallocated Sectors Count", "Số sector hỏng đã được ổ chuyển sang vùng dự phòng.",
     "Bề mặt đĩa bắt đầu hỏng vật lý; tăng dần là dấu hiệu xuống cấp.", "Chỉ số lõi của luật và đặc trưng", "5,40%", "54,6%"),
    ("smart_9_raw", "Power-On Hours", "Tổng số giờ ổ đã bật.",
     "Tuổi của ổ. Ổ nào cũng khác 0 nên không phân biệt được ổ sắp hỏng.", "Không dùng làm cờ; chỉ là đặc trưng tuổi", "99,87%", "88,2%"),
    ("smart_187_raw", "Reported Uncorrectable Errors", "Số lỗi đọc ghi mà ổ không tự sửa được (chỉ một số dòng ổ báo cáo).",
     "Dữ liệu đã không đọc được: dấu hiệu nghiêm trọng; nhiều ổ không báo thuộc tính này (null).", "Chỉ số lõi của luật", "1,58%", "22,8%"),
    ("smart_188_raw", "Command Timeout", "Số lệnh bị hủy vì quá thời gian.",
     "Ổ phản hồi chậm hoặc treo; giá trị có outlier rất lớn nên chỉ dùng thận trọng.", "Đặc trưng, không phải chỉ số lõi", "2,20%", "17,1%"),
    ("smart_194_raw", "Temperature", "Nhiệt độ ổ, độ C.",
     "Trong dữ liệu này nhiệt độ ổ hỏng và ổ khỏe gần như giống nhau, nên không có tín hiệu phân biệt.", "Đặc trưng, không dùng làm cờ", "99,87%", "không có số"),
    ("smart_197_raw", "Current Pending Sector Count", "Số sector đang chờ được chuyển đi vì đọc không ổn định.",
     "Tín hiệu mạnh nhất: có ở khoảng một nửa số ổ hỏng so với 2% ổ khỏe.", "Chỉ số lõi của luật và đặc trưng quan trọng nhất", "2,12%", "50,8%"),
    ("smart_198_raw", "Offline Uncorrectable Sector Count", "Số sector không sửa được khi ổ tự quét ở chế độ nền.",
     "Lỗi bề mặt đã xác nhận qua quét; tín hiệu khá mạnh.", "Chỉ số lõi của luật", "1,48%", "35,4%"),
    ("smart_199_raw", "UDMA CRC Error Count", "Số lỗi truyền dữ liệu qua cáp và giao tiếp.",
     "Lỗi cáp hoặc cổng chứ không phải lỗi đĩa, nên không báo trước hỏng ổ.", "Không dùng làm cờ", "1,22%", "0,97%"),
]

CONCEPTS = [
    ("Ngày-ổ (drive-day)", "Một dòng dữ liệu = một ổ trong một ngày. 30,6 triệu dòng Q1 là tổng số ngày-ổ, không phải số ổ (có 351.095 ổ)."),
    ("AFR (tỷ lệ hỏng hằng năm)", "Số lượt hỏng chia cho (số ngày-ổ chia 365). Đưa mọi nhóm về cùng cơ sở thời gian để so sánh hãng, model. Toàn Q1: 1,23%."),
    ("Raw và normalized", "Mỗi thuộc tính SMART có giá trị thô (raw: bộ đếm thật, đơn vị tùy hãng) và giá trị chuẩn hóa (normalized: thang điểm do hãng quy định, thấp hơn thường là xấu hơn). Dự án dùng giá trị raw vì ý nghĩa dễ đọc."),
    ("Null (thiếu)", "Một thuộc tính có thể không được báo bởi dòng ổ đó. Ở lớp Silver dự án giữ nguyên null (không điền 0) vì thiếu khác với bằng 0. Khi tạo đặc trưng cho mô hình, mỗi thuộc tính có thêm cờ is_missing (1 nếu thiếu), rồi giá trị thiếu mới được điền 0 để mô hình đọc được; nhờ cờ nên mô hình vẫn phân biệt thiếu với bằng 0."),
    ("Mất cân bằng lớp", "Ổ hỏng cực hiếm, nên không dùng accuracy mà dùng recall@K, precision@K và PR-AUC."),
]
