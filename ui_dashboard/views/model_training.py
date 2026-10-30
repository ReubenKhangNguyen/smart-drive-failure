from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from ui_dashboard import common, data

# Time-based split of 2026-Q1 as configured (config/project.yaml) and reported (docs/evaluation.md); the first 30 days
# only warm up the 7/14/30-day window features. Row counts are those of the Gold features table.
SPLITS = [
    ("Khởi động cửa sổ", "2026-01-01", "2026-01-30", None, "Chỉ để tính đặc trưng cửa sổ, không huấn luyện"),
    ("Train", "2026-01-31", "2026-03-03", 10842302, "Huấn luyện mô hình"),
    ("Validation", "2026-03-04", "2026-03-14", 3771986, "Chọn mô hình (và K, ngưỡng)"),
    ("Test", "2026-03-15", "2026-03-31", 3435296, "Đánh giá cuối, chạy đúng một lần"),
]
STEPS = [
    "Gán nhãn fail_within_7_days; loại 7 ngày cuối của dataset (right-censoring).",
    "Tạo 90 đặc trưng: chỉ số S.M.A.R.T., cờ thiếu dữ liệu, đặc trưng cửa sổ 7, 14, 30 ngày (chỉ dùng dữ liệu đến ngày t).",
    "Chia theo thời gian: train, validation, test (không chia ngẫu nhiên).",
    "Huấn luyện Logistic Regression và Random Forest trên train, có trọng số lớp vì ổ hỏng rất hiếm.",
    "Chọn mô hình trên validation theo recall@100, so với luật nền rules_v1.",
    "Đánh giá một lần trên test (đoạn normal) và kiểm chứng thêm trên quý II ngoài thời gian.",
]

st.title("Huấn luyện và chọn mô hình")
common.show_provenance()
st.caption("Cách nhóm huấn luyện, chia tập, chọn mô hình và đánh giá. Kết quả chi tiết ở trang Dự đoán hỏng 7 ngày.")

st.subheader("Quy trình huấn luyện")
for number, text in enumerate(STEPS, start=1):
    st.markdown("{}. {}".format(number, text))

st.subheader("Chia tập theo thời gian")
split_frame = pd.DataFrame(SPLITS, columns=["Tập", "Từ ngày", "Đến ngày", "Số dòng", "Dùng để"])
st.dataframe(split_frame.assign(**{"Số dòng": split_frame["Số dòng"].map(lambda v: "—" if pd.isna(v) else "{:,.0f}".format(v))}),
             use_container_width=True, hide_index=True)
chart_rows = split_frame.assign(**{"Từ ngày": pd.to_datetime(split_frame["Từ ngày"]), "Đến ngày": pd.to_datetime(split_frame["Đến ngày"]) + pd.Timedelta(days=1)})
st.altair_chart(
    alt.Chart(chart_rows).mark_bar(height=26).encode(
        x=alt.X("Từ ngày:T", title="Ngày (2026)"), x2="Đến ngày:T",
        y=alt.Y("Tập:N", sort=list(split_frame["Tập"]), title=None),
        color=alt.Color("Tập:N", legend=None, sort=list(split_frame["Tập"])),
        tooltip=["Tập", "Từ ngày", "Đến ngày", "Số dòng"],
    ).properties(height=170),
    use_container_width=True,
)
st.caption("Chia theo thời gian để mô hình không thấy tương lai: dự đoán ngày sau từ dữ liệu ngày trước, đúng như khi dùng thật.")

st.subheader("Chọn mô hình trên validation")
metrics = common.require("model_metrics")
if metrics is not None and len(metrics):
    views = data.metrics_views(metrics)
    st.dataframe(data.style_metrics(views["val"]), use_container_width=True, hide_index=True)
    val = views["val"]
    recall_col = [c for c in val.columns if c.startswith("Recall@")][0]
    st.altair_chart(
        alt.Chart(val).mark_bar().encode(
            y=alt.Y("Model:N", sort="-x", title=None, axis=alt.Axis(labelLimit=420)), x=alt.X(recall_col + ":Q", title=recall_col),
            color=alt.Color("Model:N", legend=None), tooltip=["Model", recall_col],
        ).properties(height=140),
        use_container_width=True,
    )
    st.caption("Mô hình được chọn theo recall@K (tiêu chí ưu tiên: mỗi ngày kiểm tra K ổ rủi ro cao nhất). Random Forest cao hơn ở PR-AUC và "
               "ROC-AUC nhưng thấp hơn ở recall@K, nên Logistic Regression được chọn; luật nền cho mức thấp nhất.")

st.subheader("Đường loss khi huấn luyện Logistic Regression")
loss = data.load_loss_history()
if loss is None:
    st.info("Chưa có lịch sử loss. Chạy scripts/export_loss_history.py (spark-submit) rồi lưu dòng LOSS_JSON vào "
            "artifacts/reports/{}.".format(data.LOSS_FILE))
else:
    history = loss["objective_history"]
    frame = pd.DataFrame({"Vòng lặp": range(len(history)), "Loss": history})
    left, middle, right = st.columns(3)
    left.metric("Loss ban đầu", "{:.3f}".format(history[0]))
    middle.metric("Loss sau 10 vòng", "{:.3f}".format(history[min(10, len(history) - 1)]))
    right.metric("Loss cuối ({} vòng)".format(len(history) - 1), "{:.3f}".format(history[-1]))
    st.altair_chart(
        alt.Chart(frame).mark_line(point=True).encode(
            x=alt.X("Vòng lặp:Q", title="Vòng lặp huấn luyện (iteration)"),
            y=alt.Y("Loss:Q", title="Hàm mất mát trên tập train", scale=alt.Scale(zero=False)),
            tooltip=["Vòng lặp", alt.Tooltip("Loss:Q", format=".4f")],
        ),
        use_container_width=True,
    )
    st.caption("Loss giảm nhanh ở những vòng đầu rồi giảm chậm dần; huấn luyện dừng ở giới hạn {} vòng. Đây là loss trên tập train "
               "(có trọng số lớp), không phải loss của validation, nên không dùng để kết luận về overfitting: bằng chứng nằm ở các "
               "chỉ số trên validation, test và quý II.".format(loss.get("max_iter", len(history) - 1)))
    st.caption("Nguồn: lần huấn luyện lại Logistic Regression với cùng cấu hình trên tập train (scripts/export_loss_history.py), vì mô hình "
               "đã lưu không giữ lịch sử loss. Mô hình chính thức không bị thay đổi.")

st.subheader("Kết quả trên tập test (đoạn normal)")
if metrics is not None and len(metrics):
    st.dataframe(data.style_metrics(views["test_normal"]), use_container_width=True, hide_index=True)
    st.caption("Không hiển thị recall@K của toàn bộ tập test: 7 ngày cuối chỉ còn ổ đã biết chắc hỏng nên mọi phương pháp đều đạt 100% "
               "ở đoạn đó (xem trang Dự đoán hỏng 7 ngày).")
