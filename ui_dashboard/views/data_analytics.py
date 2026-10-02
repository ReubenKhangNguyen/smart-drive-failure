import streamlit as st

from ui_dashboard import common, data
from ui_dashboard.explain import HEALTH_RULE_NOTE

st.title("Tình trạng ổ cứng")
common.show_provenance()
st.caption("Đầu ra 2. Mỗi ổ được xếp Khỏe / Cần theo dõi / Nguy hiểm theo luật rules_v1, kèm lý do.")
st.caption(HEALTH_RULE_NOTE)

overview, _ = common.load("dashboard_overview")
scored_date = str(overview.iloc[0]["scored_date"]) if overview is not None and len(overview) else None

st.subheader("Phân bố 3 mức và tỷ lệ hỏng thật")
distribution = common.require("health_distribution")
if distribution is not None and len(distribution):
    view = data.health_sorted(distribution)
    healthy_rate = view.loc[view["health_level"] == "HEALTHY", "failure_rate_7d"]
    base = float(healthy_rate.iloc[0]) if len(healthy_rate) and healthy_rate.iloc[0] else None
    view["Mức"] = view["health_level"].map(data.HEALTH_SHORT_LABELS)
    view["% ngày-ổ"] = view["share"] * 100
    view["% hỏng thật"] = view["failure_rate_7d"] * 100
    view["Gấp (lần)"] = view["failure_rate_7d"] / base if base else float("nan")
    st.dataframe(
        view[["Mức", "drive_days", "% ngày-ổ", "labeled_rows", "failed_within_7d", "% hỏng thật", "Gấp (lần)"]].rename(
            columns={"drive_days": "Ngày-ổ", "labeled_rows": "Đã gán nhãn", "failed_within_7d": "Hỏng ≤7 ngày"}),
        use_container_width=True, hide_index=True,
        column_config={"% ngày-ổ": st.column_config.NumberColumn(format="%.2f"),
                       "% hỏng thật": st.column_config.NumberColumn(format="%.4f"),
                       "Gấp (lần)": st.column_config.NumberColumn(format="%.1f")})
    st.caption("% hỏng thật = tỷ lệ ngày-ổ hỏng trong 7 ngày sau (đã gán nhãn); Gấp (lần) = so với mức Khỏe.")
    st.caption("Tỷ lệ hỏng thật tính trên các ngày-ổ đã loại 7 ngày cuối dataset (right-censoring). Đây là baseline của luật.")

st.subheader("Ổ cần chú ý trong ngày chấm điểm" + (" ({})".format(scored_date) if scored_date else ""))
snapshot = common.require("health_snapshot")
topk, _ = common.load("predictions_topk")
if snapshot is not None:
    levels = st.multiselect("Mức", ["CRITICAL", "WATCH"], default=["CRITICAL"], key="snap_levels")
    models = sorted(snapshot["model"].dropna().unique())
    chosen_model = st.selectbox("Model ổ", ["(tất cả)"] + models, key="snap_model")
    table = snapshot[snapshot["health_level"].isin(levels)].copy()
    if chosen_model != "(tất cả)":
        table = table[table["model"] == chosen_model]
    table["reasons"] = table["reasons"].map(data.reasons_text)
    st.write("{:,} ổ".format(len(table)))
    st.dataframe(
        table[["serial_number", "model", "health_level", "reasons"]].rename(columns=data.DISPLAY_COLUMNS),
        use_container_width=True, hide_index=True)

    st.markdown("**Tra cứu serial**")
    query = st.text_input("Serial (nhập đúng hoặc một phần)", key="snap_query").strip()
    if query:
        found = snapshot[snapshot["serial_number"].str.contains(query, case=False, regex=False)].copy()
        found["reasons"] = found["reasons"].map(data.reasons_text)
        in_topk = topk[topk["serial_number"].str.contains(query, case=False, regex=False)] if topk is not None else None
        if len(found):
            st.dataframe(found[["serial_number", "model", "health_level", "reasons"]].rename(columns=data.DISPLAY_COLUMNS),
                         use_container_width=True, hide_index=True)
        if in_topk is not None and len(in_topk):
            st.write("Có trong Top-K của ngày chấm điểm:")
            st.dataframe(in_topk[["risk_rank", "serial_number", "risk_score", "health_level"]].rename(columns=data.DISPLAY_COLUMNS),
                         use_container_width=True, hide_index=True)
        if not len(found) and (in_topk is None or not len(in_topk)):
            st.warning(
                "Không tìm thấy. Tra cứu chỉ phủ các ổ WATCH/CRITICAL và Top-K của ngày chấm điểm; ổ HEALTHY không được "
                "liệt kê (chỉ đếm ở bảng phân bố).")

st.subheader("Phân cụm hành vi SMART (K-Means)")
st.caption(
    "Phân tích mô tả hồi cứu, mẫu lệch ngày (ổ khỏe lấy ngày quan sát cuối quý, ổ hỏng lấy ngày liền trước ngày hỏng). "
    "Không so sánh trực tiếp với recall@K của mô hình dự đoán; tỷ lệ hỏng của cụm không phải xác suất hỏng.")
clusters = common.require("kmeans_clusters")
if clusters is not None and len(clusters):
    view = clusters.sort_values("cluster_id").copy()
    view["% hỏng"] = view["failed_rate"] * 100
    view["% null smart_187"] = view["smart_187_missing_share"] * 100
    st.dataframe(
        view[["cluster_id", "cluster_type", "size", "healthy_count", "failed_count", "% hỏng",
              "mean_smart_5_raw", "mean_smart_187_raw", "mean_smart_197_raw", "mean_smart_198_raw", "% null smart_187"]].rename(
            columns={"cluster_id": "Cụm", "cluster_type": "Loại", "size": "Số ổ", "healthy_count": "Khỏe", "failed_count": "Hỏng",
                     "mean_smart_5_raw": "TB smart_5", "mean_smart_187_raw": "TB smart_187",
                     "mean_smart_197_raw": "TB smart_197", "mean_smart_198_raw": "TB smart_198"}),
        use_container_width=True, hide_index=True,
        column_config={"% hỏng": st.column_config.NumberColumn(format="%.2f"),
                       "% null smart_187": st.column_config.NumberColumn(format="%.1f"),
                       "TB smart_5": st.column_config.NumberColumn(format="%.1f"),
                       "TB smart_187": st.column_config.NumberColumn(format="%.1f"),
                       "TB smart_197": st.column_config.NumberColumn(format="%.1f"),
                       "TB smart_198": st.column_config.NumberColumn(format="%.1f")})
    zero = view[view["cluster_type"] == "zero_signal"]
    if len(zero):
        st.info(
            "Nhóm zero_signal (cả 4 chỉ số bằng 0) không đưa vào K-Means; có {:,} ổ hỏng nằm trong nhóm này: đây là giới hạn "
            "của cách tiếp cận dựa trên 4 chỉ số SMART.".format(int(zero["failed_count"].iloc[0])))
selection = common.require("kmeans_k_selection")
if selection is not None and len(selection):
    left, right = st.columns(2)
    left.markdown("**Chọn K (silhouette)**")
    left.line_chart(selection.sort_values("k").set_index("k")["silhouette"])
    right.markdown("**Elbow (WSSSE)**")
    right.line_chart(selection.sort_values("k").set_index("k")["wssse"])
crosstab = common.require("kmeans_health_crosstab")
if crosstab is not None and len(crosstab):
    pivot = crosstab.pivot_table(index="cluster_id", columns="health_level", values="drives", aggfunc="sum", fill_value=0)
    pivot = pivot.reindex(columns=[c for c in data.HEALTH_ORDER if c in pivot.columns])
    st.markdown("**Cụm × mức tình trạng (số ổ)**")
    st.dataframe(pivot.reset_index().rename(columns={"cluster_id": "Cụm"}), use_container_width=True, hide_index=True)
    st.caption("K-Means dùng đúng 4 chỉ số của luật rules_v1 nên sự khớp với mức tình trạng phần lớn là hệ quả của định nghĩa.")
