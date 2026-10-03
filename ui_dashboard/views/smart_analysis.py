import altair as alt
import streamlit as st

from ui_dashboard import common

COHORT_LABELS = {"healthy": "Ổ khỏe (ngày quan sát cuối)", "pre_failure": "Ổ hỏng (ngày trước khi hỏng)"}

st.title("Phân tích chỉ số S.M.A.R.T.")
common.show_provenance()
st.caption("Đầu ra 1. Dữ liệu Silver 2026-Q1; bảng tổng hợp HĐ6.")

st.subheader("Tỷ lệ hỏng hằng năm (AFR)")
st.caption("AFR = số lượt hỏng / (số ngày-ổ / 365), tỷ lệ mỗi năm. Nhóm có ít ngày-ổ cho AFR không đáng tin.")
by_manufacturer = common.require("afr_by_manufacturer")
if by_manufacturer is not None:
    view = by_manufacturer.copy()
    view["manufacturer"] = view["manufacturer"].fillna("(không suy ra được hãng)")
    view["AFR (%)"] = view["afr"] * 100
    st.dataframe(
        view.rename(columns={"manufacturer": "Hãng", "drive_days": "Ngày-ổ", "failures": "Lượt hỏng"})[
            ["Hãng", "Ngày-ổ", "Lượt hỏng", "AFR (%)"]],
        use_container_width=True, hide_index=True)

by_model = common.require("afr_by_model")
if by_model is not None and len(by_model):
    top = int(by_model["drive_days"].max())
    minimum = st.slider("Số ngày-ổ tối thiểu của model", 0, top, min(100000, top), key="afr_min_days")
    shown = by_model[by_model["drive_days"] >= minimum].sort_values("afr", ascending=False).head(20).copy()
    shown["AFR (%)"] = shown["afr"] * 100
    st.dataframe(
        shown.rename(columns={"model": "Model", "drive_days": "Ngày-ổ", "failures": "Lượt hỏng"})[
            ["Model", "Ngày-ổ", "Lượt hỏng", "AFR (%)"]],
        use_container_width=True, hide_index=True)

st.subheader("Ổ hỏng so với ổ khỏe")
distribution = common.require("smart_distribution")
if distribution is not None and len(distribution):
    attribute = st.selectbox("Thuộc tính SMART", sorted(distribution["smart_attribute"].unique()), key="smart_attr")
    one = distribution[distribution["smart_attribute"] == attribute].copy()
    one["Nhóm"] = one["cohort"].map(COHORT_LABELS)
    st.dataframe(
        one[["Nhóm", "n", "mean", "p50", "p90", "p99"]].rename(columns={"n": "Số ổ có giá trị"}),
        use_container_width=True, hide_index=True)
    long = one.melt(id_vars=["Nhóm"], value_vars=["mean", "p50", "p90", "p99"], var_name="Thống kê", value_name="Giá trị")
    chart = alt.Chart(long).mark_bar().encode(
        x=alt.X("Thống kê:N", sort=["mean", "p50", "p90", "p99"]),
        xOffset="Nhóm:N",
        y=alt.Y("Giá trị:Q", scale=alt.Scale(type="symlog")),
        color="Nhóm:N",
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption("Trục tung dùng thang symlog vì giá trị SMART chênh nhau nhiều bậc độ lớn.")

st.subheader("Tín hiệu SMART trước khi hỏng")
signal = common.require("pre_failure_signal")
if signal is not None and len(signal):
    view = signal.copy()
    view["Cửa sổ"] = view["window_days"].map(lambda d: "{} ngày".format(d))
    view["Tỷ lệ ổ hỏng có tín hiệu (%)"] = view["signal_rate"] * 100
    chart = alt.Chart(view).mark_bar().encode(
        x=alt.X("smart_attribute:N", title="Thuộc tính"),
        xOffset="Cửa sổ:N",
        y=alt.Y("Tỷ lệ ổ hỏng có tín hiệu (%):Q"),
        color="Cửa sổ:N",
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(
        "Tín hiệu = giá trị > 0 ít nhất một lần trong cửa sổ trước ngày hỏng, trên {:,} ổ hỏng. Ổ hỏng ở đầu quý có cửa sổ "
        "30 ngày bị cắt do thiếu lịch sử.".format(int(view["failed_serials"].iloc[0])))
