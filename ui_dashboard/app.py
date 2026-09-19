"""Temporary Streamlit UI shell: it will read prepared HDFS Gold outputs and display SMART-drive analytics and seven-day failure-risk predictions."""

import os

import streamlit as st


st.set_page_config(page_title="SMART Drive Failure", page_icon="💽", layout="wide")
st.title("SMART Drive Failure")
st.caption("Big Data analytics and 7-day failure-risk prediction")

st.info(
    "The infrastructure is running. Execute ingestion and the Spark pipeline to populate "
    "the HDFS Gold layer; this UI will then display the prepared outputs."
)

left, right = st.columns(2)
left.metric("HDFS endpoint", os.environ.get("HDFS_NAMENODE_URL", "Not configured"))
right.metric("Spark endpoint", os.environ.get("SPARK_MASTER_URL", "Not configured"))
