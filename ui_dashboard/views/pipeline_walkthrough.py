from __future__ import annotations

import time

import streamlit as st

from ui_dashboard import data, walkthrough

STATUS_ICON = {"pending": "Chờ", "running": "Đang chạy", "ok": "Xong", "failed": "Lỗi"}

st.title("Quy trình 9 bước")
st.caption(
    "Mỗi bước: làm gì, lý thuyết, cần chuẩn bị, chạy bên trong như thế nào, input và output. Phần trạng thái, nhật ký và số đo "
    "lấy từ lần chạy thật của DAG `{}` trên bộ demo nhỏ tách riêng (/smart-drive-demo), không đụng dữ liệu thật.".format(walkthrough.DAG_ID))


def _evidence():
    base = data.demo_dir() / "steps"
    return {t: data.read_json(base / "{}.json".format(t)) for t, _, _ in walkthrough.TASKS}


def _fmt_seconds(value) -> str:
    if value is None:
        return "—"
    return "{:.0f} giây".format(value) if value < 120 else "{:.1f} phút".format(value / 60)


@st.fragment(run_every=3)
def live_timeline() -> None:
    """Redraws every 3 s so the viewer sees the steps change colour while Airflow runs them."""
    evidence = _evidence()
    run = data.read_json(data.demo_dir() / "run.json")
    if not any(evidence.values()):
        st.info("Chưa có lần chạy demo. Mở Airflow ({}), bật DAG `{}` rồi bấm Trigger; trang này tự cập nhật.".format(
            walkthrough.AIRFLOW_URL, walkthrough.DAG_ID))
        return
    if run:
        st.caption("Lần chạy {} | trạng thái: {} | bắt đầu {}".format(
            run.get("run_id", "?"), STATUS_ICON.get(run.get("status"), run.get("status")), run.get("started_at", "?")))
    rows = []
    for task, label, title in walkthrough.TASKS:
        item = evidence.get(task) or {}
        rows.append({"Bước": label, "Nội dung": title, "Trạng thái": STATUS_ICON.get(item.get("status"), "Chờ"),
                     "Thời gian": _fmt_seconds(item.get("seconds"))})
    st.dataframe(rows, use_container_width=True, hide_index=True)
    done = sum(1 for i in evidence.values() if i and i.get("status") == "ok")
    st.progress(done / len(walkthrough.TASKS), text="{}/{} tác vụ xong".format(done, len(walkthrough.TASKS)))


st.subheader("Tiến trình chạy")
live_timeline()
st.caption("Theo dõi đồ thị tác vụ ở Airflow: {} | file HDFS: {} | Spark: {}".format(
    walkthrough.AIRFLOW_URL, walkthrough.HDFS_URL, walkthrough.SPARK_URL))

SPEEDS = {"Chậm": 0.12, "Vừa": 0.04, "Nhanh": 0.012}
DONE_KEY = "walk_done"  # how many of the nine steps the viewer has already run


def _replay(task: str, saved: dict, delay: float, box, bar) -> None:
    """Prints the recorded log of one task line by line, like the code running again, at a chosen speed.
    The text is the real log of the run above (nothing is invented), only shown faster."""
    lines = saved.get("lines", [])
    shown = []
    for index, line in enumerate(lines, start=1):
        shown.append(line)
        box.code("\n".join(shown[-28:]), language="text")
        bar.progress(index / max(len(lines), 1), text="{}: dòng {}/{}".format(task, index, len(lines)))
        time.sleep(delay if line else delay / 3)


def _step_tasks(item: dict) -> list:
    return [(t, l, n) for t, l, n in walkthrough.TASKS if t in item["tasks"]]


st.subheader("Chạy thử từng bước")
st.caption(
    "Làm lần lượt từng bước: bước sau bị khóa cho đến khi bạn chạy xong bước trước, vì đầu ra của bước trước là đầu vào của bước sau. "
    "Các dòng xử lý chạy ra từng dòng cho đến khi xong. Nội dung là nhật ký thật của lần chạy ở trên, chỉ được phát nhanh hơn.")
walk_data = _evidence()
ready = all((walk_data.get(t) or {}).get("status") == "ok" for t, _, _ in walkthrough.TASKS)
if not ready:
    st.info("Cần một lần chạy DAG hoàn chỉnh (13 tác vụ xong) thì mới chạy thử từng bước được.")
else:
    done = int(st.session_state.get(DONE_KEY, 0))
    top = st.columns([3, 1])
    speed = top[0].radio("Tốc độ", list(SPEEDS), index=1, horizontal=True, key="replay_speed")
    if top[1].button("Làm lại từ đầu", use_container_width=True, disabled=done == 0):
        st.session_state[DONE_KEY] = 0
        st.rerun()
    st.progress(done / len(walkthrough.STEPS), text="Đã xong {}/{} bước".format(done, len(walkthrough.STEPS)))

    for item in walkthrough.STEPS:
        number = item["step"]
        tasks = _step_tasks(item)
        if number <= done:
            with st.expander("Bước {}. {} — đã xong".format(number, item["title"]), expanded=number == done):
                for task, label, name in tasks:
                    saved = walk_data[task]
                    st.markdown("**{} - {}** ({})".format(label, name, _fmt_seconds(saved.get("seconds"))))
                    st.code("\n".join(saved.get("lines", [])[-60:]), language="text")
                st.caption("Output: " + item["output"])
        elif number == done + 1:
            st.markdown("#### Bước {}. {}".format(number, item["title"]))
            st.markdown("**Mục tiêu.** " + item["goal"])
            previous = walkthrough.STEPS[number - 2]["output"] if number > 1 else "cụm HDFS đã khởi động"
            st.caption("Input lấy từ: {}".format(previous))
            if st.button("Chạy bước {}".format(number), type="primary", key="run_step_{}".format(number)):
                for task, label, name in tasks:
                    st.markdown("**{} - {}**".format(label, name))
                    _replay(task, walk_data[task], SPEEDS[speed], st.empty(), st.empty())
                st.session_state[DONE_KEY] = number
                st.rerun()
        else:
            st.markdown("Bước {}. {}: cần chạy xong bước {} trước".format(number, item["title"], number - 1))

    if done == len(walkthrough.STEPS):
        st.success("Đã chạy hết 9 bước. Kết quả cuối của luồng nằm ở các trang Tổng quan, Tình trạng ổ cứng và Dự đoán hỏng 7 ngày.")

st.subheader("Dòng chảy dữ liệu")
st.dataframe([{"Từ": a, "Đến": b, "Bước": c} for a, b, c in walkthrough.CHAIN], use_container_width=True, hide_index=True)

st.subheader("Chi tiết từng bước")
evidence_now = _evidence()
for item in walkthrough.STEPS:
    with st.expander("Bước {}. {}".format(item["step"], item["title"])):
        st.markdown("**Mục tiêu.** " + item["goal"])
        cols = st.columns(2)
        with cols[0]:
            st.markdown("**Lý thuyết**")
            for line in item["theory"]:
                st.markdown("- " + line)
            st.markdown("**Cần chuẩn bị**")
            for line in item["prepare"]:
                st.markdown("- " + line)
        with cols[1]:
            st.markdown("**Bên trong chạy gì**")
            for line in item["inside"]:
                st.markdown("- " + line)
            st.markdown("**Input:** " + item["input"])
            st.markdown("**Output:** " + item["output"])
        for task in item["tasks"]:
            saved = evidence_now.get(task)
            label = next(l for t, l, _ in walkthrough.TASKS if t == task)
            if not saved or saved.get("status") == "pending":
                st.caption("Tác vụ {} ({}): chưa chạy.".format(label, task))
                continue
            st.markdown("**Kết quả chạy thật, tác vụ {} ({})** — {} trong {}".format(
                label, task, STATUS_ICON.get(saved.get("status")), _fmt_seconds(saved.get("seconds"))))
            if saved.get("error"):
                st.error(saved["error"])
            st.code("\n".join(saved.get("lines", [])[-80:]) or "(chưa có nhật ký)", language="text")
            if saved.get("metrics"):
                st.json(saved["metrics"], expanded=False)
