# gui.py  (Stage 2 updated — selectable biomass channels)
import pandas as pd
import streamlit as st
import datetime
import db_pg

st.set_page_config(page_title="Stage 2 — Reactors Dashboard (Select Channels)", layout="wide")
st.title("Stage 2 — Reactors Dashboard (Select biomass channels)")

# Ensure DB tables exist
db_pg.ensure_db()

def get_conn():
    return db_pg.get_pg_conn()

@st.cache_data(ttl=2)
def load_recent(reactor: str, tag: str, minutes: int):
    conn = get_conn()
    try:
        since = (datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes)).isoformat()
        q = """
        SELECT s.ts_utc, s.value
        FROM samples s
        JOIN experiments e ON s.experiment_id = e.id
        WHERE e.reactor = %s AND s.tag = %s AND s.ts_utc >= %s
        ORDER BY s.ts_utc ASC
        """
        df = pd.read_sql_query(q, conn, params=(reactor, tag, since))
        if len(df) == 0:
            return df
        df["ts_utc"] = pd.to_datetime(df["ts_utc"])
        df = df.set_index("ts_utc")
        return df
    finally:
        conn.close()

@st.cache_data(ttl=2)
def list_available_tags(reactor: str, limit=1000):
    conn = get_conn()
    try:
        q = """
        SELECT DISTINCT s.tag
        FROM samples s
        JOIN experiments e ON s.experiment_id = e.id
        WHERE e.reactor = %s
        ORDER BY s.tag ASC
        LIMIT %s
        """
        df = pd.read_sql_query(q, conn, params=(reactor, limit))
        return df["tag"].tolist()
    finally:
        conn.close()

# Sidebar controls
with st.sidebar:
    st.header("Plot Controls")
    reactor = st.selectbox("Reactor", ["R0","R1","R2"])
    window_min = st.slider("Time window (minutes)", 1, 120, 10, step=1)
    st.caption("Select biomass channels to plot (multi-select)")

# available tags (discovered from DB)
available_tags = list_available_tags(reactor)
# default biomass channels in order
default_biomass = [
    "biomass_415","biomass_445","biomass_480","biomass_515","biomass_555",
    "biomass_590","biomass_630","biomass_680","biomass_clear","biomass_nir"
]
# intersection of available tags and defaults, for sensible defaults
default_selection = [t for t in default_biomass if t in available_tags]
if not default_selection:
    # if DB doesn't yet have biomass tags, fall back to available tags
    default_selection = available_tags[:3] if available_tags else []

selected = st.multiselect("Select channels to plot", options=available_tags, default=default_selection)

if not selected:
    st.info("No channels selected. Choose one or more biomass channels from the multi-select.")
    st.stop()

# Latest metrics: read the most recent value per tag for quick glance
def load_latest_value(conn, reactor, tag):
    q = """
    SELECT s.ts_utc, s.value FROM samples s
    JOIN experiments e ON s.experiment_id = e.id
    WHERE e.reactor = %s AND s.tag = %s
    ORDER BY s.ts_utc DESC LIMIT 1
    """
    df = pd.read_sql_query(q, conn, params=(reactor, tag))
    if df.empty:
        return None
    return float(df.iloc[0]["value"])

conn = get_conn()
try:
    latest_vals = {tag: load_latest_value(conn, reactor, tag) for tag in ["ph_pH","do_ppm"] + selected}
finally:
    conn.close()

# display top metrics (pH / DO + first selected)
m1, m2, m3 = st.columns(3)
m1.metric(f"{reactor} pH", "—" if latest_vals.get("ph_pH") is None else f"{latest_vals['ph_pH']:.3f}")
m2.metric(f"{reactor} DO (ppm)", "—" if latest_vals.get("do_ppm") is None else f"{latest_vals['do_ppm']:.3f}")
first_sel = selected[0] if selected else None
m3.metric(f"{reactor} {first_sel}", "—" if latest_vals.get(first_sel) is None else f"{latest_vals[first_sel]:.3f}")

st.divider()

# Build combined DataFrame for selected channels
dfs = []
for tag in selected:
    df_tag = load_recent(reactor, tag, window_min)
    if df_tag.empty:
        continue
    df_tag = df_tag.rename(columns={"value": tag})
    dfs.append(df_tag)

if not dfs:
    st.info("No data available for the selected channels in the chosen time window.")
else:
    # join on timestamp (outer), then plot
    combined = pd.concat(dfs, axis=1)
    combined = combined.dropna(how="all")
    if combined.empty:
        st.info("Data present but after aligning timestamps nothing remained (try a larger time window).")
    else:
        st.subheader(f"{reactor} — Selected biomass channels (last {window_min} min)")
        st.line_chart(combined)

st.divider()
st.subheader("Latest actuator parameters (logged)")
# load latest actuator values
conn = get_conn()
try:
    tags_act = ["pwm0_setpoint","pwm0_lb","pwm0_ub"]
    latest_act = {}
    for t in tags_act:
        r = pd.read_sql_query("""
            SELECT s.value FROM samples s
            JOIN experiments e ON s.experiment_id = e.id
            WHERE e.reactor=%s AND s.tag=%s ORDER BY s.ts_utc DESC LIMIT 1
        """, conn, params=(reactor,t))
        latest_act[t] = None if r.empty else float(r.iloc[0]["value"])
finally:
    conn.close()

a1, a2, a3 = st.columns(3)
a1.metric("Setpoint", "—" if latest_act["pwm0_setpoint"] is None else f"{latest_act['pwm0_setpoint']:.3f}")
a2.metric("LB", "—" if latest_act["pwm0_lb"] is None else f"{latest_act['pwm0_lb']:.3f}")
a3.metric("UB", "—" if latest_act["pwm0_ub"] is None else f"{latest_act['pwm0_ub']:.3f}")

with st.expander("Raw recent rows for reactor"):
    conn = get_conn()
    try:
        q = """
        SELECT s.ts_utc, s.tag, s.nodeid, s.value
        FROM samples s
        JOIN experiments e ON s.experiment_id = e.id
        WHERE e.reactor = %s
        ORDER BY s.ts_utc DESC
        LIMIT 500
        """
        df = pd.read_sql_query(q, conn, params=(reactor,))
        st.dataframe(df)
    finally:
        conn.close()