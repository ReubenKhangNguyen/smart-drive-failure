import altair as alt
import streamlit as st

from ui_dashboard import common, data

st.title("Hiệu năng cụm")
common.show_provenance()
st.caption("Cụm Docker Compose: 1 NameNode, 3 DataNode, 1 Spark Master, 2 Spark Worker (cấu hình, không phải số đo).")

st.subheader("Thời gian chạy các bước pipeline")
runs = data.list_pipeline_runs()
if runs:
    chosen = st.selectbox("Nhật ký chạy", [r.name for r in runs], key="pipeline_run")
    st.markdown(next(r for r in runs if r.name == chosen).read_text(encoding="utf-8"))
else:
    st.info("Chưa có nhật ký chạy. Chạy `spark-submit scripts/run_batch_pipeline.py` để sinh `artifacts/reports/pipeline_run_<ngày>.md`.")

st.subheader("Benchmark Big Data")
st.caption(
    "Mỗi biến thể đo 1 lần khởi động không tính rồi 3 lần tính trung vị (xen kẽ giữa các biến thể), với bộ nhớ đệm hệ điều hành "
    "đã ấm; dòng có Lần = 1 là một lần chạy duy nhất, không phải trung vị. Số đo chạy trên một laptop với Docker, không đại diện "
    "cho cụm thật.")
benchmark = common.require("benchmark")
if benchmark is not None and len(benchmark):
    for experiment, table in data.benchmark_views(benchmark).items():
        st.markdown("**{}**".format(data.BENCHMARK_TITLES[experiment]))
        st.dataframe(table, use_container_width=True, hide_index=True)
        part = benchmark[benchmark["experiment"] == experiment]
        if len(part) > 1:
            st.altair_chart(
                alt.Chart(part).mark_bar().encode(
                    y=alt.Y("variant:N", sort="-x", title=None, axis=alt.Axis(labelLimit=320)),
                    x=alt.X("median_seconds:Q", title="Trung vị (giây)"),
                    tooltip=["variant", "query", "runs", "median_seconds", "min_seconds", "max_seconds"],
                ),
                use_container_width=True,
            )
    environment = common.require("benchmark_environment")
    if environment is not None and len(environment):
        st.markdown("**Cấu hình máy và cụm lúc đo**")
        st.dataframe(environment.rename(columns={"item": "Mục", "value": "Giá trị"}), use_container_width=True, hide_index=True)
