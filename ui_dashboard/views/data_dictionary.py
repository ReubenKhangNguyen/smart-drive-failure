from __future__ import annotations

import pandas as pd
import streamlit as st

from ui_dashboard import common, dictionary

st.title("Dữ liệu và thuộc tính")
common.show_provenance()
st.caption("Bộ dữ liệu Backblaze Drive Stats: mỗi ngày một file, mỗi dòng là trạng thái của một ổ cứng trong ngày đó. "
           "Trang này giải thích từng cột có nghĩa gì và nói lên điều gì về sức khỏe ổ.")

overview = common.require("dashboard_overview")
if overview is not None and len(overview):
    row = overview.iloc[0]
    a, b, c, d = st.columns(4)
    a.metric("Số ổ", "{:,}".format(int(row["drive_count"])))
    b.metric("Số ngày-ổ (số dòng)", "{:,}".format(int(row["drive_days"])))
    c.metric("Số lượt hỏng", "{:,}".format(int(row["failure_count"])))
    d.metric("Số cột trong CSV gốc", "197")
    st.caption("Mỗi file CSV có 197 cột (5 cột cơ bản cùng các cặp thuộc tính SMART thô và chuẩn hóa); dự án giữ 13 cột cần dùng ở lớp Silver.")

st.subheader("Cột cơ bản")
st.table(pd.DataFrame(dictionary.BASE_COLUMNS, columns=["Cột", "Ý nghĩa", "Mô tả", "Vai trò trong dự án"]).set_index("Cột"))

st.subheader("Thuộc tính S.M.A.R.T. được dùng")
st.caption("S.M.A.R.T. là cơ chế tự giám sát của ổ cứng, báo các bộ đếm sức khỏe. Số liệu dưới đây là tỷ lệ ổ có giá trị lớn hơn 0 "
           "trong Q1/2026: ở nhóm ổ khỏe và ở nhóm ổ hỏng (trong 7 ngày trước khi hỏng).")
smart = pd.DataFrame(dictionary.SMART, columns=["Thuộc tính", "Tên", "Đếm gì", "Nói lên điều gì", "Vai trò trong dự án", "Ổ khỏe có giá trị > 0", "Ổ hỏng có giá trị > 0"])
st.table(smart.set_index("Thuộc tính"))
st.info("Cách đọc: một chỉ số có ích khi tỷ lệ ở ổ hỏng cao hơn nhiều so với ổ khỏe. smart_197 là ví dụ rõ nhất (50,8% so với 2,12%). "
        "smart_9, smart_194 và smart_199 hầu như không phân biệt được hai nhóm nên không dùng làm cờ cảnh báo. Nguồn: docs/health_rules.md.")

st.subheader("Cột do dự án tạo ra")
st.table(pd.DataFrame(dictionary.DERIVED_COLUMNS, columns=["Cột", "Ý nghĩa", "Cách tạo"]).set_index("Cột"))

st.subheader("Khái niệm cần nắm")
for term, text in dictionary.CONCEPTS:
    st.markdown("**{}.** {}".format(term, text))
