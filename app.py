# app.py
"""
Streamlit HMI (revised)
- Uses OpcWorker + Request from opc_worker.py
- Method dropdown writes integer based on mapping
- Plots use Altair scatter charts in long-form (avoids colon encoding errors)
- Reads snapshot from worker.latest_values via RPC
"""

import streamlit as st
from datetime import datetime, timezone, timedelta
from queue import Queue
import sqlite3
import pandas as pd
import altair as alt

from opc_worker import OpcWorker, Request

# Endpoint and DB defaults
ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
DB_PATH_DEFAULT = "data/stage2.sqlite"

# Method mapping (word -> int)
METHOD_CHOICES = {"manual": 0, "timer": 1, "on_boundaries": 2, "pid": 3}


def _db_connect(db_path: str):
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def db_list_experiments(db_path: str) -> pd.DataFrame:
    try:
        with _db_connect(db_path) as con:
            df = pd.read_sql_query("SELECT id, name, reactor, started_at_utc FROM experiments ORDER BY id DESC", con)
        return df
    except Exception:
        return pd.DataFrame(columns=["id", "name", "reactor", "started_at_utc"])


def db_list_tags(db_path: str, experiment_id: int) -> list:
    try:
        with _db_connect(db_path) as con:
            rows = con.execute("SELECT DISTINCT tag FROM samples WHERE experiment_id = ? ORDER BY tag", (experiment_id,)).fetchall()
        return [r["tag"] for r in rows if r["tag"]]
    except Exception:
        return []


def db_load_timeseries(db_path: str, experiment_id: int, tags: list, hours: int) -> pd.DataFrame:
    if not tags:
        return pd.DataFrame(columns=["ts_utc", "tag", "value"])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=int(hours))
    placeholders = ",".join(["?"] * len(tags))
    params = [experiment_id, cutoff.isoformat(), *tags]
    query = f"""
        SELECT ts_utc, tag, value
        FROM samples
        WHERE experiment_id = ?
          AND ts_utc >= ?
          AND tag IN ({placeholders})
        ORDER BY ts_utc ASC
    """
    try:
        with _db_connect(db_path) as con:
            df = pd.read_sql_query(query, con, params=params)
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
        df = df.dropna(subset=["ts_utc"])
        return df
    except Exception:
        return pd.DataFrame(columns=["ts_utc", "tag", "value"])


# Page config
st.set_page_config(page_title="Reactors HMI (Stage1+2)", layout="wide")
st.title("Reactors HMI — Live Control + Logging + Visualization")
st.caption(f"Server endpoint: {ENDPOINT}")

# Sidebar controls (auto-refresh)
st.sidebar.header("Auto-refresh")
auto_on = st.sidebar.checkbox("Enable auto-refresh", value=False)
auto_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=1, max_value=10, value=3)

# Start worker if necessary and store in session state
if "opc_worker" not in st.session_state:
    w = OpcWorker()
    w.start()
    st.session_state["opc_worker"] = w

worker: OpcWorker = st.session_state["opc_worker"]


def rpc_connect_and_browse(worker: OpcWorker, timeout: float = 25):
    reply_q = Queue()
    req = Request(kind="connect_browse", endpoint=ENDPOINT, variables={}, payload=None, reply_q=reply_q)
    return worker.request(req, timeout=timeout)


def rpc_read_snapshot(worker: OpcWorker, timeout: float = 10):
    reply_q = Queue()
    req = Request(kind="read_snapshot", endpoint=ENDPOINT, variables={}, payload=None, reply_q=reply_q)
    return worker.request(req, timeout=timeout)


def rpc_write(worker: OpcWorker, writes: dict, timeout: float = 20):
    reply_q = Queue()
    req = Request(kind="write", endpoint=ENDPOINT, variables={}, payload=writes, reply_q=reply_q)
    return worker.request(req, timeout=timeout)


def rpc_call(worker: OpcWorker, method_nodeid: str, timeout: float = 20):
    reply_q = Queue()
    req = Request(kind="call", endpoint=ENDPOINT, variables={}, payload=method_nodeid, reply_q=reply_q)
    return worker.request(req, timeout=timeout)


# Connect + browse controls
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    if st.button("Connect + Browse server"):
        res = rpc_connect_and_browse(worker)
        if res.get("ok"):
            st.success("Browse OK (address space captured).")
            # worker.latest_values may now contain initial values
        else:
            st.error(f"Browse failed: {res.get('error')}")

with col2:
    if st.button("Refresh values (snapshot)"):
        res = rpc_read_snapshot(worker)
        if res.get("ok"):
            st.success("Snapshot read OK")
            # store snapshot in session state for UI
            st.session_state["last_snapshot"] = res.get("data", {})
        else:
            st.error(f"Snapshot failed: {res.get('error')}")

with col3:
    if st.button("Stop worker / Disconnect"):
        worker.stop()
        st.session_state.pop("opc_worker", None)
        st.warning("Worker stopped. Reload page to restart.")


# read snapshot automatically if auto_on
if auto_on:
    # simple periodic snapshot
    if "auto_tick" not in st.session_state:
        st.session_state["auto_tick"] = 0
    st.session_state["auto_tick"] += 1
    if st.session_state["auto_tick"] % max(1, int(auto_interval)) == 0:
        res = rpc_read_snapshot(worker)
        if res.get("ok"):
            st.session_state["last_snapshot"] = res.get("data", {})


# Tabs for reactors (R0,R1,R2)
tabs = st.tabs(["R0", "R1", "R2"])
# Show only R0 content fully for now (R1/R2 placeholders)
for i, reactor in enumerate(["R0", "R1", "R2"]):
    with tabs[i]:
        st.header(f"{reactor}")
        # Snapshot store
        last = st.session_state.get("last_snapshot", worker.latest_values.copy())

        st.subheader("Live sensor values (from browsed address space)")
        # Build a small DataFrame for display: nodeid, tag, value
        rows = []
        for nodeid, val in last.items():
            # tag formatting: if you already have mapping in your client, modify accordingly
            # Here assume tag is known as a stored label in last values (if not, use nodeid)
            tag = getattr(val, "tag", None) if hasattr(val, "tag") else None
            # If val is a primitive, we only have nodeid and value
            rows.append({"nodeid": nodeid, "tag": tag or nodeid, "value": val})
        if rows:
            df_show = pd.DataFrame(rows)
            st.dataframe(df_show, use_container_width=True, height=300)
        else:
            st.info("No snapshot available yet. Click Connect + Browse or Refresh values.")

        st.divider()
        st.subheader("Actuator controls — pwm0")
        # Basic pwm0 form (method dropdown writes integer per METHOD_CHOICES)
        with st.form(f"{reactor}_pwm0_form"):
            cols = st.columns(2)
            with cols[0]:
                method_choice = st.selectbox(f"{reactor} pwm0 method", options=list(METHOD_CHOICES.keys()), index=1)
                time_on_in = st.number_input(f"{reactor} pwm0 time_on (s)", value=0.0, key=f"{reactor}_time_on")
                time_off_in = st.number_input(f"{reactor} pwm0 time_off (s)", value=0.0, key=f"{reactor}_time_off")
            with cols[1]:
                lb_in = st.number_input(f"{reactor} pwm0 lb", value=0.0, key=f"{reactor}_lb")
                ub_in = st.number_input(f"{reactor} pwm0 ub", value=100.0, key=f"{reactor}_ub")
                setpoint_in = st.number_input(f"{reactor} pwm0 setpoint", value=0.0, key=f"{reactor}_setpoint")
            write_btn = st.form_submit_button("Write pwm0 for " + reactor)

        if write_btn:
            # build writes dict: we need nodeids for each variable. We expect user to have browsed mappings
            # We try to infer nodeids from last snapshot: nodeids that contain group names like pwm0:method etc.
            # This is heuristic: adjust to how ReactorOpcClient stores mapping.
            candidate_writes = {}
            # build a simple heuristic over existing nodeids
            for nid in last.keys():
                if f"{reactor}:pwm0:method" in str(nid) or f"{reactor}:pwm0:method" in str(last.get(nid)):
                    candidate_writes[nid] = METHOD_CHOICES[method_choice]
                # fallback by text matching of nodeid string
                if "method" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = METHOD_CHOICES[method_choice]
                if "time_on" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = float(time_on_in)
                if "time_off" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = float(time_off_in)
                if "lb" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = float(lb_in)
                if "ub" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = float(ub_in)
                if "setpoint" in str(nid) and reactor in str(nid) and "pwm0" in str(nid):
                    candidate_writes[nid] = float(setpoint_in)

            # If no nodeids found, warn and skip
            if not candidate_writes:
                st.error("Could not find pwm0 nodeids in snapshot. Make sure you have browsed the server and the mappings exist.")
            else:
                res = rpc_write(worker, candidate_writes)
                if res.get("ok"):
                    st.success("Write OK")
                else:
                    st.error(f"Write failed: {res.get('error')}")

        st.divider()
        st.subheader("Methods")
        c1, c2 = st.columns(2)
        with c1:
            if st.button(f"Call set_pairing ({reactor})"):
                # Attempt to discover a 'set_pairing' method nodeid
                # Heuristic over snapshot tags
                method_nid = None
                for nid in last.keys():
                    if "set_pairing" in str(nid):
                        method_nid = nid
                        break
                if not method_nid:
                    st.error("set_pairing NodeId not found in snapshot")
                else:
                    res = rpc_call(worker, method_nid)
                    if res.get("ok"):
                        st.success(f"set_pairing OK: {res.get('data')}")
                    else:
                        st.error(f"set_pairing failed: {res.get('error')}")
        with c2:
            if st.button(f"Call unpair ({reactor})"):
                method_nid = None
                for nid in last.keys():
                    if "unpair" in str(nid):
                        method_nid = nid
                        break
                if not method_nid:
                    st.error("unpair NodeId not found in snapshot")
                else:
                    res = rpc_call(worker, method_nid)
                    if res.get("ok"):
                        st.success("unpair OK")
                    else:
                        st.error(f"unpair failed: {res.get('error')}")

        st.info("R1/R2 will work once the server mappings are discovered or variable_map entries added.")

# ----- Stage 2: Logging & Plots (four-panel scatter) -----
st.header("Stage 2 — Logging & Plots (from SQLite/Postgres)")

with st.expander("Plot settings", expanded=True):
    db_path = st.text_input("DB path (sqlite or connection string)", value=DB_PATH_DEFAULT)
    time_window_h = st.slider("Time window (hours)", min_value=1, max_value=24, value=6, step=1)

    exp_df = db_list_experiments(db_path)
    if exp_df.empty:
        st.warning("No experiments found. Run the sampler to create an experiment and samples.")
        st.stop()

    exp_df["label"] = exp_df.apply(lambda r: f"#{int(r['id'])} | {r['reactor']} | {r['name']} | {r['started_at_utc']}", axis=1)
    chosen_label = st.selectbox("Experiment", exp_df["label"].tolist())
    chosen_id = int(exp_df.loc[exp_df["label"] == chosen_label, "id"].iloc[0])

    all_tags = db_list_tags(db_path, chosen_id)
    # default biomass channel heuristics
    default_tag = None
    for pref in ["R0:biomass:415", "R0:biomass:480", "R0:biomass:555"]:
        if pref in all_tags:
            default_tag = pref
            break
    if default_tag is None and all_tags:
        default_tag = all_tags[0]

    biomass_tag = st.selectbox("Biomass channel", options=all_tags, index=all_tags.index(default_tag) if default_tag in all_tags else 0)

# Load timeseries for plotting. We will fetch all tags used in panels:
# For pH, DO, temp find tags containing ':ph:', ':do:', ':oC'
# For biomass use the selected biomass_tag
ph_tags = [t for t in all_tags if ":ph:" in t]
do_tags = [t for t in all_tags if ":do:" in t]
temp_tags = [t for t in all_tags if ":oC" in t or ":oC" in t.lower()]
biomass_tags = [biomass_tag] if biomass_tag else []

plot_tags = list(set(ph_tags + do_tags + temp_tags + biomass_tags))

df_ts = db_load_timeseries(db_path, chosen_id, plot_tags, time_window_h)

if df_ts.empty:
    st.info("No samples found for the selected experiment/time window.")
else:
    # Long-form DataFrame (ts_utc, tag, value)
    df_plot = df_ts.copy()
    df_plot["tag_label"] = df_plot["tag"].astype(str)  # keep original tag text for legend

    # Ensure ts_utc is datetime
    df_plot["ts_utc"] = pd.to_datetime(df_plot["ts_utc"], utc=True)

    # Build charts (scatter) for each panel
    st.subheader("R0 pH (pH) / R0 DO (ppm)")
    c1, c2 = st.columns(2)
    with c1:
        subset = df_plot[df_plot["tag"].str.contains(":ph:")]
        if not subset.empty:
            chart = alt.Chart(subset).mark_point().encode(
                x=alt.X("ts_utc:T", title="Time (UTC)"),
                y=alt.Y("value:Q", title="pH"),
                color=alt.Color("tag_label:N", title="Series"),
                tooltip=["ts_utc:T", "tag_label:N", "value:Q"]
            ).properties(width=600, height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No pH channels available for this experiment.")

    with c2:
        subset = df_plot[df_plot["tag"].str.contains(":do:")]
        if not subset.empty:
            chart = alt.Chart(subset).mark_point().encode(
                x=alt.X("ts_utc:T", title="Time (UTC)"),
                y=alt.Y("value:Q", title="DO (ppm)"),
                color=alt.Color("tag_label:N", title="Series"),
                tooltip=["ts_utc:T", "tag_label:N", "value:Q"]
            ).properties(width=600, height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No DO channels available for this experiment.")

    st.subheader("R0 Temperature (°C) / R0 Biomass")
    c3, c4 = st.columns(2)
    with c3:
        subset = df_plot[df_plot["tag"].str.contains(":oC") | df_plot["tag"].str.contains(":oC".lower())]
        if not subset.empty:
            chart = alt.Chart(subset).mark_point().encode(
                x=alt.X("ts_utc:T", title="Time (UTC)"),
                y=alt.Y("value:Q", title="Temperature (°C)"),
                color=alt.Color("tag_label:N", title="Series"),
                tooltip=["ts_utc:T", "tag_label:N", "value:Q"]
            ).properties(width=600, height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No temperature channels available for this experiment.")

    with c4:
        subset = df_plot[df_plot["tag"].isin(biomass_tags)]
        if not subset.empty:
            chart = alt.Chart(subset).mark_point().encode(
                x=alt.X("ts_utc:T", title="Time (UTC)"),
                y=alt.Y("value:Q", title="Biomass"),
                color=alt.Color("tag_label:N", title="Series"),
                tooltip=["ts_utc:T", "tag_label:N", "value:Q"]
            ).properties(width=600, height=250)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No biomass samples available for this selection.")

    with st.expander("Raw samples (latest 200)", expanded=False):
        st.dataframe(df_plot.tail(200), use_container_width=True)