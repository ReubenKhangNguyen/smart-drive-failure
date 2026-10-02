import altair as alt
import pandas as pd
import streamlit as st

from ui_dashboard import common, data
from ui_dashboard.explain import RISK_SCORE_NOTE, SCORED_DAY_NOTE, TAIL_NOTE, conflict_note, missing_flag_note, tie_note

SMART_COLUMNS = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]

st.title("Dự đoán nguy cơ hỏng trong 7 ngày")
common.show_provenance()
st.caption("Đầu ra 3. Mô hình Logistic Regression xếp hạng các ổ theo điểm rủi ro; Top-K là danh sách ưu tiên kiểm tra.")
st.warning(RISK_SCORE_NOTE)

overview, _ = common.load("dashboard_overview")
k = int(overview.iloc[0]["k"]) if overview is not None and len(overview) else 100
scored_date = str(overview.iloc[0]["scored_date"]) if overview is not None and len(overview) else None

st.subheader("Top-{} ổ có điểm rủi ro cao nhất".format(k) + (", ngày {}".format(scored_date) if scored_date else ""))
st.caption(SCORED_DAY_NOTE)
topk = common.require("predictions_topk")
snapshot, _ = common.load("health_snapshot")
if topk is not None and len(topk):
    notes = {}
    for _, row in topk.iterrows():
        kind, _text = conflict_note(row["health_level"] if pd.notna(row["health_level"]) else None, bool(row["alert"]), k)
        if kind == "conflict":
            notes[row["serial_number"]] = "Mâu thuẫn với luật (xem giải thích bên dưới)"
    st.dataframe(
        data.topk_display(topk, notes), use_container_width=True, hide_index=True,
        column_config={"Điểm rủi ro": st.column_config.NumberColumn(format="%.6f")})
    ties = tie_note(topk["risk_score"], k)
    if ties:
        st.warning(ties)

    summary = data.conflict_summary(topk, snapshot)
    st.markdown("**Top-K × mức tình trạng của luật**")
    rows = [(level, summary["topk_by_level"].get(level, 0)) for level in data.HEALTH_ORDER + ["(không có)"]
            if summary["topk_by_level"].get(level, 0)]
    st.dataframe(pd.DataFrame(rows, columns=["Mức tình trạng", "Số ổ trong Top-{}".format(k)]),
                 use_container_width=True, hide_index=True)
    if "critical_total" in summary:
        st.caption(
            "Trong ngày này luật xếp {:,} ổ CRITICAL, trong đó {:,} ổ nằm trong Top-{} và {:,} ổ nằm ngoài Top-{} "
            "(Top-K bị giới hạn bởi năng lực kiểm tra).".format(
                summary["critical_total"], summary["critical_in_topk"], k, summary["critical_outside_topk"], k))

    st.markdown("**Xem một ổ: mức tình trạng, điểm rủi ro và lịch sử SMART**")
    serial = st.selectbox("Serial trong Top-K", list(topk.sort_values("risk_rank")["serial_number"]), key="topk_serial")
    chosen = topk[topk["serial_number"] == serial].iloc[0]
    level = chosen["health_level"] if pd.notna(chosen["health_level"]) else None
    st.write("Hạng {} | điểm rủi ro {:.4f} | mức tình trạng: {}".format(
        int(chosen["risk_rank"]), float(chosen["risk_score"]), data.HEALTH_LABELS.get(level, "(không có)")))
    kind, text = conflict_note(level, bool(chosen["alert"]), k)
    if text:
        {"conflict": st.error, "agree": st.success}.get(kind, st.info)(text)
    history = common.require("smart_history_topk")
    if history is not None:
        one = history[history["serial_number"] == serial].sort_values("date")
        if len(one):
            st.caption("Mỗi chỉ số một biểu đồ với thang riêng (các chỉ số chênh nhau nhiều bậc độ lớn). 30 ngày gần nhất tới ngày chấm điểm; "
                       "chỗ trống là ngày không có giá trị (null, không điền 0).")
            left, right = st.columns(2)
            for index, name in enumerate(SMART_COLUMNS):
                box = left if index % 2 == 0 else right
                series = one.set_index("date")[name]
                box.markdown("**{}**".format(name))
                if series.notna().any():
                    box.line_chart(series, height=200)
                else:
                    box.caption("Không có giá trị (null) trong 30 ngày; không điền 0.")

st.subheader("Chất lượng mô hình (K = {})".format(k))
metrics = common.require("model_metrics")
if metrics is not None and len(metrics):
    views = data.metrics_views(metrics)
    st.markdown("**Tập test, đoạn normal (2026-03-15 đến 03-24): số chính để đánh giá**")
    st.dataframe(data.style_metrics(views["test_normal"]), use_container_width=True, hide_index=True)
    st.caption("Recall@K và precision@K của toàn bộ tập test không được hiển thị: đoạn tail (right-censoring) chi phối con số đó. "
               "Xem giải thích ở mục đoạn tail bên dưới.")
    if len(views["normal_size"]):
        size = views["normal_size"].iloc[0]
        st.caption("Đoạn normal: {:,} dòng, {:,} dòng nhãn dương (gần như toàn bộ tập test).".format(
            int(size["rows"]), int(size["positives"])))
    with st.expander("Đoạn tail (2026-03-25 đến 03-31) và vì sao không dùng số toàn test"):
        st.dataframe(data.style_metrics(views["test_tail"]), use_container_width=True, hide_index=True)
        if len(views["tail_size"]):
            size = views["tail_size"].iloc[0]
            st.caption("Đoạn tail chỉ có {:,} dòng, {:,} dòng nhãn dương.".format(int(size["rows"]), int(size["positives"])))
        st.info(TAIL_NOTE)
    st.markdown("**Tập validation (dùng để chọn mô hình)**")
    st.dataframe(data.style_metrics(views["val"]), use_container_width=True, hide_index=True)
    if len(views["test_auc"]):
        st.markdown("**PR-AUC / ROC-AUC trên toàn tập test (ổn định so với validation)**")
        st.dataframe(data.style_metrics(views["test_auc"]), use_container_width=True, hide_index=True)

st.subheader("Đặc trưng quan trọng")
importance = common.require("model_feature_importance")
if importance is not None and len(importance):
    for model_name in importance["model"].drop_duplicates():
        part = importance[importance["model"] == model_name].sort_values("rank")
        st.markdown("**{}**".format(data.MODEL_LABELS.get(model_name, model_name)))
        st.caption(str(part["source"].iloc[0]))
        st.altair_chart(
            alt.Chart(part).mark_bar().encode(
                y=alt.Y("feature:N", sort=list(part["feature"]), title=None, axis=alt.Axis(labelLimit=400)),
                x=alt.X("importance:Q", title="importance"),
                tooltip=["rank", "feature", "importance"],
            ),
            use_container_width=True,
        )
    st.caption("Giá trị importance của hai mô hình không so sánh được với nhau.")
    lr_features = importance.loc[importance["model"] == "logistic_regression", "feature"]
    flag_note = missing_flag_note(lr_features)
    if flag_note:
        st.info(flag_note)
