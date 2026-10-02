import streamlit as st

from ui_dashboard import common, data

st.title("Tổng quan")
st.caption("Phân tích dữ liệu S.M.A.R.T. và dự đoán nguy cơ hỏng ổ cứng trong 7 ngày tới (Backblaze Drive Stats, 2026-Q1).")

overview = common.require("dashboard_overview")
if overview is not None and len(overview):
    row = overview.iloc[0]
    a, b, c, d = st.columns(4)
    a.metric("Số ổ", "{:,}".format(int(row["drive_count"])))
    b.metric("Số ngày-ổ", "{:,}".format(int(row["drive_days"])))
    c.metric("Số lượt hỏng", "{:,}".format(int(row["failure_count"])))
    d.metric("AFR (mỗi năm)", common.pct(row["afr"]))
    e, f, g, h = st.columns(4)
    e.metric("Dữ liệu từ", str(row["data_start"]))
    f.metric("đến", str(row["data_end"]))
    g.metric("Mô hình", str(row["model_version"]))
    h.metric("Ngày chấm điểm", str(row["scored_date"]))

distribution, _ = common.load("health_distribution")
if distribution is not None:
    st.subheader("Tình trạng ổ cứng (toàn quý)")
    view = data.health_sorted(distribution)
    view["Mức"] = view["health_level"].map(data.HEALTH_LABELS)
    st.bar_chart(view.set_index("Mức")["share"] * 100, y_label="% số ngày-ổ")

st.subheader("Trạng thái dữ liệu của dashboard")
st.caption("Bảng nào thiếu thì trang liên quan hiện hướng dẫn tạo bảng đó.")
st.dataframe(data.table_status(), use_container_width=True, hide_index=True)
