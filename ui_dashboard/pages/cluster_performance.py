import streamlit as st

from ui_dashboard import common, data

st.title("Hiệu năng cụm")
st.caption("Cụm Docker Compose: 1 NameNode, 3 DataNode, 1 Spark Master, 2 Spark Worker (cấu hình, không phải số đo).")

st.subheader("Thời gian chạy các bước pipeline")
runs = data.list_pipeline_runs()
if runs:
    chosen = st.selectbox("Nhật ký chạy", [r.name for r in runs], key="pipeline_run")
    st.markdown(next(r for r in runs if r.name == chosen).read_text(encoding="utf-8"))
else:
    st.info("Chưa có nhật ký chạy. Chạy `spark-submit scripts/run_batch_pipeline.py` để sinh `artifacts/reports/pipeline_run_<ngày>.md`.")

st.subheader("Benchmark Big Data")
benchmark = common.require("benchmark")
if benchmark is not None:
    st.dataframe(benchmark, use_container_width=True, hide_index=True)
