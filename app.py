import asyncio
import datetime
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from client import ReactorOpcClient

# -------------------------
# CONFIG
# -------------------------
ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
DB_PATH = Path("data/stage2.sqlite")

# R0 NodeIds (authoritative for your mock server + requirements)
R0 = {
    "object": "ns=2;i=1",
    "ph": "ns=2;i=3",
    "do": "ns=2;i=6",
    "bio_415": "ns=2;i=9",
    "setpoint": "ns=2;i=28",
    "lb": "ns=2;i=26",
    "ub": "ns=2;i=27",
    "time_on": "ns=2;i=24",
    "time_off": "ns=2;i=25",
    "method": "ns=2;i=23",
    "set_pairing": "ns=2;i=232",
}

BIOMASS_TAGS = [
    "biomass_415","biomass_445","biomass_480","biomass_515","biomass_555",
    "biomass_590","biomass_630","biomass_680","biomass_clear","biomass_nir"
]

# -------------------------
# STREAMLIT SETUP
# -------------------------
st.set_page_config(page_title="Reactors HMI (Stage 1 + Stage 2)", layout="wide")
st.title("Reactors HMI — Stage 1 (Control) + Stage 2 (Logging/Plots)")

# -------------------------
# SINGLE EVENT LOOP (CRITICAL)
# -------------------------
if "loop" not in st.session_state:
    st.session_state.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(st.session_state.loop)

def run(coro):
    return st.session_state.loop.run_until_complete(coro)

# -------------------------
# OPC CLIENT (Stage 1)
# -------------------------
if "client" not in st.session_state:
    st.session_state.client = ReactorOpcClient(ENDPOINT)
if "connected" not in st.session_state:
    st.session_state.connected = False

# -------------------------
# DB HELPERS (Stage 2)
# -------------------------
def get_conn():
    return sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)

@st.cache_data(ttl=2)
def list_available_reactors():
    if not DB_PATH.exists():
        return []
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT DISTINCT reactor FROM samples ORDER BY reactor", conn)
        return df["reactor"].tolist()
    finally:
        conn.close()

@st.cache_data(ttl=2)
def list_available_tags(reactor: str):
    if not DB_PATH.exists():
        return []
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT DISTINCT tag FROM samples WHERE reactor=? ORDER BY tag",
            conn,
            params=(reactor,),
        )
        return df["tag"].tolist()
    finally:
        conn.close()

@st.cache_data(ttl=2)
def load_recent(reactor: str, tag: str, minutes: int):
    conn = get_conn()
    try:
        since = (datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes)).isoformat()
        q = """
        SELECT ts_utc, value
        FROM samples
        WHERE reactor = ? AND tag = ? AND ts_utc >= ?
        ORDER BY ts_utc ASC
        """
        df = pd.read_sql_query(q, conn, params=(reactor, tag, since))
        if df.empty:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"])
        df = df.set_index("ts_utc")
        return df
    finally:
        conn.close()

@st.cache_data(ttl=2)
def load_latest(reactor: str, tag: str):
    conn = get_conn()
    try:
        q = """
        SELECT ts_utc, value
        FROM samples
        WHERE reactor = ? AND tag = ?
        ORDER BY ts_utc DESC
        LIMIT 1
        """
        df = pd.read_sql_query(q, conn, params=(reactor, tag))
        if df.empty:
            return None
        return float(df.iloc[0]["value"])
    finally:
        conn.close()

# -------------------------
# TOP BAR: Connection + status
# -------------------------
top1, top2, top3 = st.columns([1, 2, 2])

with top1:
    if st.button("Connect OPC-UA"):
        try:
            run(st.session_state.client.connect())
            st.session_state.connected = True
            st.success("Connected")
        except Exception as e:
            st.session_state.connected = False
            st.error(f"Connect failed: {e}")

with top2:
    st.caption(f"OPC-UA endpoint: {ENDPOINT}")

with top3:
    if DB_PATH.exists():
        st.caption(f"DB: {DB_PATH}")
    else:
        st.caption("DB: not found (run sampler.py to create/populate)")

st.divider()

# -------------------------
# TABS: Stage 1 + Stage 2
# -------------------------
tab1, tab2 = st.tabs(["Stage 1 — Live Control", "Stage 2 — Logging & Plots"])

# =========================
# TAB 1: LIVE CONTROL
# =========================
with tab1:
    if not st.session_state.connected:
        st.warning("Start server (python -u mock_server.py), then click 'Connect OPC-UA'.")
        st.stop()

    c = st.session_state.client

    # Live reads
    try:
        ph = run(c.read(R0["ph"]))
        do = run(c.read(R0["do"]))
        bio = run(c.read(R0["bio_415"]))
    except Exception as e:
        st.error(f"Live read failed: {e}")
        st.stop()

    m1, m2, m3 = st.columns(3)
    m1.metric("R0 pH", ph)
    m2.metric("R0 DO (ppm)", do)
    m3.metric("R0 Biomass 415", f"{bio:.3f}" if isinstance(bio, (float,int)) else str(bio))

    st.subheader("R0 Actuator Controls (pwm0 / ControlMethod)")

    try:
        curr_setpoint = float(run(c.read(R0["setpoint"])))
        curr_lb = float(run(c.read(R0["lb"])))
        curr_ub = float(run(c.read(R0["ub"])))
        curr_ton = float(run(c.read(R0["time_on"])))
        curr_toff = float(run(c.read(R0["time_off"])))
        curr_method = str(run(c.read(R0["method"])))
    except Exception as e:
        st.error(f"Failed to read control params: {e}")
        st.stop()

    new_setpoint = st.number_input("setpoint", value=curr_setpoint)
    new_lb = st.number_input("lb", value=curr_lb)
    new_ub = st.number_input("ub", value=curr_ub)
    new_ton = st.number_input("time_on (s)", value=curr_ton)
    new_toff = st.number_input("time_off (s)", value=curr_toff)
    new_method = st.text_input("method", value=curr_method)

    a, b = st.columns(2)

    with a:
        if st.button("Write controls"):
            try:
                run(c.write(R0["setpoint"], new_setpoint))
                run(c.write(R0["lb"], new_lb))
                run(c.write(R0["ub"], new_ub))
                run(c.write(R0["time_on"], new_ton))
                run(c.write(R0["time_off"], new_toff))
                run(c.write(R0["method"], new_method))
                st.success("Wrote values")
            except Exception as e:
                st.error(f"Write failed: {e}")

    with b:
        if st.button("Call set_pairing"):
            try:
                res = run(c.call_method(R0["object"], R0["set_pairing"]))
                st.write("Result:", res)
            except Exception as e:
                st.error(f"Method call failed: {e}")

# =========================
# TAB 2: LOGGING & PLOTS
# =========================
with tab2:
    if not DB_PATH.exists():
        st.warning("No DB found yet. Start logging in another terminal: python sampler.py 1")
        st.stop()

    reactors = list_available_reactors()
    if not reactors:
        st.warning("DB exists but has no data. Start sampler.py and wait ~5 seconds.")
        st.stop()

    left, right = st.columns([1, 3])

    with left:
        reactor = st.selectbox("Reactor", reactors)
        minutes = st.slider("Time window (minutes)", 1, 180, 10, step=1)

        tags = list_available_tags(reactor)
        # default biomass selection if present
        default = [t for t in BIOMASS_TAGS if t in tags]
        if not default:
            default = tags[:3] if tags else []

        selected = st.multiselect("Plot channels", options=tags, default=default)

    with right:
        st.subheader(f"{reactor} — plots (last {minutes} min)")

        if not selected:
            st.info("Select at least one channel to plot.")
        else:
            dfs = []
            for tag in selected:
                df = load_recent(reactor, tag, minutes)
                if df.empty:
                    continue
                dfs.append(df.rename(columns={"value": tag}))
            if not dfs:
                st.info("No data in that time window yet. Increase the window or wait for sampler.")
            else:
                combined = pd.concat(dfs, axis=1).dropna(how="all")
                st.line_chart(combined)

        st.divider()
        st.subheader("Latest values")
        show_tags = ["ph_pH", "do_ppm", "biomass_415", "pwm0_setpoint", "pwm0_lb", "pwm0_ub"]
        cols = st.columns(3)
        for i, tag in enumerate(show_tags):
            val = load_latest(reactor, tag) if tag in tags else None
            cols[i % 3].metric(tag, "—" if val is None else f"{val:.3f}")

        with st.expander("Raw rows (latest 200)"):
            conn = get_conn()
            try:
                df = pd.read_sql_query(
                    "SELECT ts_utc, tag, nodeid, value FROM samples WHERE reactor=? ORDER BY ts_utc DESC LIMIT 200",
                    conn,
                    params=(reactor,),
                )
                st.dataframe(df)
            finally:
                conn.close()
