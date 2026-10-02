from __future__ import annotations

from typing import Optional, Tuple

# Moi chu ve risk_score phai la "diem rui ro", khong phai xac suat (docs/decisions.md, 2026-10-02).
RISK_SCORE_NOTE = (
    "Điểm rủi ro (risk_score) là điểm xếp hạng của mô hình Logistic Regression có trọng số lớp, "
    "không phải xác suất hỏng đã hiệu chỉnh. Chỉ dùng thứ hạng và nhóm Top-K để ưu tiên kiểm tra; "
    "không đọc điểm như \"x% khả năng hỏng\"."
)

SCORED_DAY_NOTE = (
    "Ngày chấm điểm được chọn để minh họa dashboard (ngày cuối dataset 2026-03-31 chỉ còn ổ đã xác nhận hỏng, "
    "không đại diện). Ngày này nằm trong tập test nhưng không dùng để đánh giá lại hay chọn mô hình."
)

TAIL_NOTE = (
    "Đoạn tail (7 ngày cuối của tập test) bị ảnh hưởng bởi right-censoring: ổ khỏe ở đoạn này bị loại vì chưa quan "
    "sát đủ 7 ngày tương lai, nhưng ổ đã xác nhận hỏng vẫn được giữ lại, nên mọi dòng đều có nhãn dương. Vì vậy "
    "mọi phương pháp, kể cả luật đơn giản, đều đạt 100% ở đoạn này; đó là hiệu ứng chọn mẫu, không phải năng lực "
    "của mô hình. Số đáng tin để đánh giá là đoạn normal."
)

HEALTH_RULE_NOTE = (
    "Mức tình trạng do luật rules_v1 quyết định, chỉ nhìn 4 chỉ số SMART của đúng ngày đó (smart_5, 187, 197, 198). "
    "Điểm rủi ro do mô hình tính, còn dùng thêm xu hướng 7/14/30 ngày và các thuộc tính khác."
)


def conflict_note(health_level: Optional[str], alert: bool, k: int = 100) -> Tuple[Optional[str], Optional[str]]:
    """(kind, explanation) for one drive on the scored day. kind: 'agree' | 'conflict' | 'info' | None.
    None means rules and ranking do not contradict each other (HEALTHY and outside Top-K)."""
    if health_level is None:
        return "info", "Không có mức tình trạng (HĐ5) cho ổ này ở ngày chấm điểm."
    if health_level == "CRITICAL" and alert:
        return "agree", "Đồng thuận: luật xếp CRITICAL và điểm rủi ro nằm trong Top-{}. Ưu tiên kiểm tra cao.".format(k)
    if health_level == "WATCH" and alert:
        return "agree", ("Điểm rủi ro nằm trong Top-{} và luật xếp WATCH (một chỉ số SMART đã bất thường): "
                         "hai nguồn cùng cảnh báo, nên kiểm tra.".format(k))
    if health_level == "HEALTHY" and alert:
        return "conflict", ("Mâu thuẫn: điểm rủi ro nằm trong Top-{} nhưng luật xếp HEALTHY. Luật chỉ xét giá trị 4 chỉ số SMART "
                            "của ngày hôm nay (đều bằng 0), còn mô hình xét thêm xu hướng và các thuộc tính khác nên vẫn "
                            "xếp hạng cao. Cần kiểm tra thêm, không thể kết luận chỉ từ điểm rủi ro.".format(k))
    if health_level == "CRITICAL" and not alert:
        return "conflict", ("Mâu thuẫn: luật xếp CRITICAL nhưng điểm rủi ro nằm ngoài Top-{}. Top-K bị giới hạn bởi năng lực "
                            "kiểm tra ({} ổ mỗi ngày), nên ổ vẫn cần theo dõi theo luật.".format(k, k))
    if health_level == "WATCH" and not alert:
        return "info", "Luật xếp WATCH nhưng điểm rủi ro nằm ngoài Top-{}: tiếp tục theo dõi theo luật.".format(k)
    return None, None


def tie_note(scores, k: int = 100) -> Optional[str]:
    """Note when most Top-K drives share the saturated top score (risk_score == 1.0). The displayed score
    cannot tell them apart; their rank comes from the model margin (logit), then serial_number."""
    values = [float(v) for v in scores]
    if not values:
        return None
    tied = sum(1 for v in values if v >= 1.0 - 1e-9)
    if tied < len(values) / 2.0:
        return None
    return ("{} trong {} ổ của Top-{} có điểm rủi ro đúng bằng 1.0 (mô hình bão hòa), nên điểm hiển thị không phân biệt "
            "được chúng. Thứ hạng giữa các ổ này được xếp theo margin của mô hình (logit, không hiển thị ở bảng), "
            "rồi mới theo số serial nếu margin cũng bằng nhau. Ổ khác cũng đạt 1.0 nhưng margin thấp hơn có thể nằm "
            "ngoài Top-{}.".format(tied, len(values), k, k))
