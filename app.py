# app.py
import streamlit as st
from datetime import datetime, timezone, timedelta
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import altair as alt
from streamlit_autorefresh import st_autorefresh

from opc_worker import OpcWorker, Request
import db_pg

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
DB_TYPE = db_pg.ensure_db()

METHOD_LABELS = ["manual", "timer", "on_boundaries", "pid"]
METHOD_TO_INT = {"manual": 0, "timer": 1, "on_boundaries": 2, "pid": 3}


# -------------------------
# Worker RPC helpers
# -------------------------
def _rpc(worker: OpcWorker, kind: str, payload: Any = None, timeout: float = 25.0):
    reply_q = Queue()
    req = Request(kind=kind, endpoint=ENDPOINT, variables=None, payload=payload, reply_q=reply_q)
    return worker.request(req, timeout=timeout)


def rpc_connect_browse(worker: OpcWorker, timeout: float = 25.0):
    return _rpc(worker, "connect_browse", payload=None, timeout=timeout)


def rpc_read_snapshot(worker: OpcWorker, timeout: float = 10.0):
    return _rpc(worker, "read_snapshot", payload=None, timeout=timeout)


def rpc_write(worker: OpcWorker, writes: Dict[str, Any], timeout: float = 20.0):
    return _rpc(worker, "write", payload=writes, timeout=timeout)


def rpc_call(worker: OpcWorker, method_nodeid: str, args: Optional[List[Any]] = None, timeout: float = 20.0):
    """
    Supports two worker implementations:
      A) worker expects a string nodeid (old)
      B) worker expects {"nodeid":..., "args":[...]} (new)
    We'll try (B) first, then fall back to (A).
    """
    if args is None:
        args = []
    res = _rpc(worker, "call", payload={"nodeid": method_nodeid, "args": args}, timeout=timeout)
    if isinstance(res, dict) and res.get("ok"):
        return res
    # fallback
    res2 = _rpc(worker, "call", payload=method_nodeid, timeout=timeout)
    return res2


# -------------------------
# UI helpers
# -------------------------
def _mappings_loaded() -> bool:
    return (
        "mappings" in st.session_state
        and isinstance(st.session_state["mappings"], dict)
        and "sensor_vars" in st.session_state["mappings"]
        and "actuator_vars" in st.session_state["mappings"]
        and "methods" in st.session_state["mappings"]
    )


def _get_maps():
    mappings = st.session_state.get("mappings", {}) if _mappings_loaded() else {}
    sensor_vars = mappings.get("sensor_vars", {}) or {}
    actuator_vars = mappings.get("actuator_vars", {}) or {}
    methods = mappings.get("methods", {}) or {}
    return sensor_vars, actuator_vars, methods


def _snapshot() -> Dict[str, Any]:
    return st.session_state.get("last_values", {}) or {}


def _fmt_tag(info: Dict[str, Any]) -> str:
    # required structure: {"reactor":"R0","name":"do","channel":"ppm",...}
    r = info.get("reactor", "")
    n = info.get("name", "")
    c = info.get("channel", "")
    return f"{r}:{n}:{c}".strip(":")


def _method_iter(methods: Dict[str, Any]):
    # methods dict may contain non-dicts; guard hard
    for nid, info in (methods or {}).items():
        if isinstance(info, dict):
            yield nid, info


def find_method_nodeid(methods: Dict[str, Any], reactor: str, name_exact: str) -> Optional[str]:
    for nid, info in _method_iter(methods):
        if info.get("reactor") == reactor and info.get("name") == name_exact:
            return nid
    return None


def find_calibration_method(methods: Dict[str, Any], reactor: str, sensor_name: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Matches method entries like:
      {"reactor":"R0","name":"ph:calibration"}  OR  {"name":"sid:calibration"} (sid includes sensor id)
    We'll match on:
      - correct reactor
      - "calibration" in name
      - sensor_name contained in name (best-effort)
    """
    s = (sensor_name or "").lower()
    for nid, info in _method_iter(methods):
        if info.get("reactor") != reactor:
            continue
        n = (info.get("name") or "").lower()
        if "calibration" not in n:
            continue
        if s and s in n:
            return nid, info
    # fallback: first calibration method for reactor if any (better than crashing)
    for nid, info in _method_iter(methods):
        if info.get("reactor") == reactor and "calibration" in (info.get("name") or "").lower():
            return nid, info
    return None, None


def altair_scatter(df: pd.DataFrame, title: str, y_label: str):
    """
    df columns: ts_utc (datetime), value (float)
    """
    if df.empty:
        st.info("No samples in selected window.")
        return
    chart = (
        alt.Chart(df)
        .mark_circle(size=35)
        .encode(
            x=alt.X("ts_utc:T", title="Time (UTC)"),
            y=alt.Y("value:Q", title=y_label),
            tooltip=[alt.Tooltip("ts_utc:T", title="ts"), alt.Tooltip("value:Q", title="value")],
        )
        .properties(height=260, title=title)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


# -------------------------
# Page config
# -------------------------
st.set_page_config(page_title="Reactors HMI (Stage 1 + 2 + Calibration)", layout="wide")
st.title("Reactors HMI — Stage 1 (live) + Stage 2 (logging) + Calibration")
st.caption(f"Server endpoint: {ENDPOINT}")

# Sidebar: auto-refresh
st.sidebar.header("Auto-refresh")
auto_on = st.sidebar.checkbox("Enable auto-refresh", value=False)
auto_interval = st.sidebar.slider("Refresh interval (seconds)", 1, 10, 3)
if auto_on:
    st_autorefresh(interval=auto_interval * 1000, key="auto_refresh")

# Start worker once
if "opc_worker" not in st.session_state:
    w = OpcWorker()
    w.start()
    st.session_state["opc_worker"] = w

worker: OpcWorker = st.session_state["opc_worker"]

if "last_values" not in st.session_state:
    st.session_state["last_values"] = {}
if "last_snapshot_ts" not in st.session_state:
    st.session_state["last_snapshot_ts"] = None

# Top controls
top1, top2, top3, top4 = st.columns([1.2, 1.2, 1.4, 1.2])

with top1:
    if st.button("Connect + Browse server"):
        res = rpc_connect_browse(worker, timeout=30)
        if res.get("ok"):
            st.session_state["mappings"] = res.get("mappings", {})
            st.success("Browse OK (address space captured).")
        else:
            st.error(f"Browse failed: {res.get('error')}")

with top2:
    if st.button("Refresh values (snapshot)"):
        res = rpc_read_snapshot(worker, timeout=10)
        if res.get("ok"):
            st.session_state["last_values"] = res.get("data", {})
            st.session_state["last_snapshot_ts"] = datetime.now(timezone.utc).isoformat()
            st.success("Snapshot OK")
        else:
            st.error(f"Snapshot failed: {res.get('error')}")

with top3:
    st.write(f"DB backend: **{DB_TYPE}**")
    ts = st.session_state.get("last_snapshot_ts") or "—"
    st.write(f"Last snapshot: {ts}")

with top4:
    if st.button("Stop worker / Disconnect"):
        worker.stop()
        st.session_state.pop("opc_worker", None)
        st.warning("Worker stopped. Reload page to restart.")

# If auto-refresh is ON, refresh snapshot (only if we already connected/browsed once)
if auto_on and _mappings_loaded():
    res = rpc_read_snapshot(worker, timeout=10)
    if res.get("ok"):
        st.session_state["last_values"] = res.get("data", {})
        st.session_state["last_snapshot_ts"] = datetime.now(timezone.utc).isoformat()

sensor_vars, actuator_vars, methods = _get_maps()
snap = _snapshot()

# Reactor tabs (requirement)
tabs = st.tabs(["R0", "R1", "R2"])


def render_reactor_tab(reactor: str):
    st.header(f"{reactor} — Live controls & status")

    if not _mappings_loaded():
        st.info("No address space loaded yet. Click **Connect + Browse server** first.")
        return

    # -------------------------
    # Address space table (sensors + selected actuators)
    # -------------------------
    st.subheader("Live sensor values (from browsed address space)")

    rows = []
    for nid, info in (sensor_vars or {}).items():
        if not isinstance(info, dict):
            continue
        if info.get("reactor") != reactor:
            continue
        rows.append(
            {
                "nodeid": nid,
                "tag": _fmt_tag(info),
                "value": snap.get(nid, info.get("value", None)),
            }
        )

    if not rows:
        st.info("No sensor variables found for this reactor (check server address space / browse logic).")
    else:
        df = pd.DataFrame(rows).sort_values(by=["tag"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    # -------------------------
    # Actuators: tabs per actuator (requirement)
    # -------------------------
    st.subheader("Actuator controls")
    # group by actuator name (pwm0..pwm3)
    act_by_name: Dict[str, Dict[str, str]] = {}
    for nid, info in (actuator_vars or {}).items():
        if not isinstance(info, dict):
            continue
        if info.get("reactor") != reactor:
            continue
        name = info.get("name", "")
        ch = info.get("channel", "")
        if not name or not name.startswith("pwm"):
            continue
        act_by_name.setdefault(name, {})
        act_by_name[name][ch] = nid

    if not act_by_name:
        st.warning("No pwm mappings found for this reactor.")
    else:
        actuator_names = sorted(act_by_name.keys())  # pwm0..pwm3
        act_tabs = st.tabs(actuator_names)

        for t, act_name in zip(act_tabs, actuator_names):
            with t:
                nodes = act_by_name[act_name]

                # Show current values (read-only)
                cur = {k: snap.get(nid, None) for k, nid in nodes.items()}
                st.caption(f"Current (subscription snapshot): {cur}")

                with st.form(f"{reactor}_{act_name}_form"):
                    colA, colB = st.columns(2)
                    with colA:
                        method_label = st.selectbox(
                            f"{reactor} {act_name} method",
                            options=METHOD_LABELS,
                            index=1,
                            key=f"{reactor}_{act_name}_method",
                        )
                        time_on = st.number_input(
                            f"{reactor} {act_name} time_on (s)",
                            value=float(cur.get("time_on") or 0.0),
                            key=f"{reactor}_{act_name}_time_on",
                        )
                        time_off = st.number_input(
                            f"{reactor} {act_name} time_off (s)",
                            value=float(cur.get("time_off") or 0.0),
                            key=f"{reactor}_{act_name}_time_off",
                        )
                    with colB:
                        lb = st.number_input(
                            f"{reactor} {act_name} lb",
                            value=float(cur.get("lb") or 0.0),
                            key=f"{reactor}_{act_name}_lb",
                        )
                        ub = st.number_input(
                            f"{reactor} {act_name} ub",
                            value=float(cur.get("ub") or 100.0),
                            key=f"{reactor}_{act_name}_ub",
                        )
                        setpoint = st.number_input(
                            f"{reactor} {act_name} setpoint",
                            value=float(cur.get("setpoint") or 0.0),
                            key=f"{reactor}_{act_name}_setpoint",
                        )

                    submit = st.form_submit_button(f"Write {act_name} for {reactor}")

                if submit:
                    writes: Dict[str, Any] = {}

                    if "method" in nodes:
                        writes[nodes["method"]] = METHOD_TO_INT.get(method_label, 0)

                    for field, val in [
                        ("time_on", float(time_on)),
                        ("time_off", float(time_off)),
                        ("lb", float(lb)),
                        ("ub", float(ub)),
                        ("setpoint", float(setpoint)),
                    ]:
                        if field in nodes:
                            writes[nodes[field]] = val

                    if not writes:
                        st.warning("Nothing to write (no matching NodeIds for these fields).")
                    else:
                        res = rpc_write(worker, writes)
                        if res.get("ok"):
                            st.success("Write OK")
                        else:
                            st.error(f"Write failed: {res.get('error')}")

    st.divider()

    # -------------------------
    # Methods: set_pairing / unpair (requirement)
    # -------------------------
    st.subheader("Methods")
    m1, m2 = st.columns(2)

    with m1:
        if st.button("Call set_pairing", key=f"{reactor}_set_pairing"):
            nid = find_method_nodeid(methods, reactor, "set_pairing")
            if not nid:
                st.error("set_pairing method not found for reactor.")
            else:
                res = rpc_call(worker, nid, args=[])
                if res.get("ok"):
                    st.success(f"set_pairing OK: {res.get('data')}")
                else:
                    st.error(f"set_pairing failed: {res.get('error')}")

    with m2:
        if st.button("Call unpair", key=f"{reactor}_unpair"):
            nid = find_method_nodeid(methods, reactor, "unpair")
            if not nid:
                st.error("unpair method not found for reactor.")
            else:
                res = rpc_call(worker, nid, args=[])
                if res.get("ok"):
                    st.success(f"unpair OK: {res.get('data')}")
                else:
                    st.error(f"unpair failed: {res.get('error')}")

    st.divider()

    # -------------------------
    # Calibration (new requirements)
    # Each sensor gets CP1/CP2 tabs + history
    # -------------------------
    st.subheader("Calibration")
    st.caption("Run sensor calibrations (CP1 / CP2). Results are stored in the database and shown here.")

    # Available sensors derived from browsed sensor_vars
    # Keep it practical: only (ph, do) channels + biomass channels selectable
    sensor_options = []
    for nid, info in (sensor_vars or {}).items():
        if not isinstance(info, dict):
            continue
        if info.get("reactor") != reactor:
            continue
        name = info.get("name", "")
        ch = info.get("channel", "")
        if name in {"ph", "do", "biomass"}:
            sensor_options.append(f"{name}:{ch}")

    sensor_options = sorted(set(sensor_options))
    if not sensor_options:
        st.info("No sensors found for calibration on this reactor.")
        return

    sel = st.selectbox("Sensor", options=sensor_options, key=f"{reactor}_cal_sensor")
    sensor_name, sensor_channel = sel.split(":", 1)

    # Find calibration method nodeid (from browsed methods dictionary)
    cal_nid, cal_info = find_calibration_method(methods, reactor, sensor_name)

    tab_cp1, tab_cp2, tab_hist = st.tabs(["CP1", "CP2", "History"])

    def run_calibration(cp_label: str):
        if not cal_nid:
            st.error("Calibration method not found on server for this sensor (mock server may not expose it yet).")
            return

        point = st.number_input(
            f"Calibration point ({cp_label})",
            value=0.0,
            key=f"{reactor}_{sel}_{cp_label}_point",
        )
        val = st.number_input(
            f"Calibration value ({cp_label})",
            value=0.0,
            key=f"{reactor}_{sel}_{cp_label}_value",
        )

        if st.button(f"Run calibration {cp_label}", key=f"{reactor}_{sel}_{cp_label}_run"):
            res = rpc_call(worker, cal_nid, args=[float(point), float(val)])
            if not res.get("ok"):
                st.error(f"Calibration call failed: {res.get('error')}")
                return

            out = res.get("data")

            # expected output: [str, float, float]
            status = str(out)
            quality = 0.0
            returned_value = float(val)

            try:
                if isinstance(out, (list, tuple)) and len(out) >= 3:
                    status = str(out[0])
                    quality = float(out[1])
                    returned_value = float(out[2])
            except Exception:
                pass

            ts_iso = datetime.now(timezone.utc).isoformat()

            # store
            db_pg.insert_calibration(
                ts_iso=ts_iso,
                reactor=reactor,
                sensor=sel,          # keep full "ph:pH" etc
                cp=cp_label.lower(), # cp1/cp2
                point=float(point),
                value=float(val),
                status=status,
                quality=float(quality),
                returned_value=float(returned_value),
                method_nodeid=cal_nid,
            )

            st.success("Calibration recorded")
            st.write(
                {
                    "timestamp_utc": ts_iso,
                    "status": status,
                    "quality": quality,
                    "returned_value": returned_value,
                    "method_nodeid": cal_nid,
                    "method_name": (cal_info or {}).get("name") if isinstance(cal_info, dict) else None,
                }
            )

    with tab_cp1:
        run_calibration("CP1")

    with tab_cp2:
        run_calibration("CP2")

    with tab_hist:
        last_n = st.number_input(
            "Show last N calibrations",
            min_value=1,
            max_value=200,
            value=20,
            key=f"{reactor}_{sel}_hist_n",
        )
        rows = db_pg.list_calibrations(reactor=reactor, sensor=sel, limit=int(last_n))
        if not rows:
            st.info("No calibrations recorded yet.")
        else:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


for i, tab in enumerate(tabs):
    with tab:
        render_reactor_tab(["R0", "R1", "R2"][i])

# -------------------------
# Stage 2 plots (requirements: 1h..24h, scatter, 4 plots)
# -------------------------
st.divider()
st.subheader("Stage 2 — Logging & Plots (from DB)")

experiments = db_pg.list_experiments()
if not experiments:
    st.warning("No experiments found. Run the sampler to create experiments/samples.")
    st.stop()

exp_labels = [f"#{e['id']} | {e['reactor']} | {e['name']} | {e['started_at_utc']}" for e in experiments]
sel_label = st.selectbox("Experiment", exp_labels, key="plot_exp")
sel_id = int(sel_label.split("|")[0].strip().lstrip("#"))
exp_reactor = sel_label.split("|")[1].strip()

tags = db_pg.list_tags(sel_id)
if not tags:
    st.warning("No tags found for this experiment.")
    st.stop()

hours = st.slider("Time window (hours)", min_value=1, max_value=24, value=6, step=1)
minutes = int(hours * 60)

# Identify default tags for 4 plots
# Expected tag format: "R0:ph:pH", "R0:ph:oC", "R0:do:ppm", "R0:do:oC", "R0:biomass:415", ...
def pick_tag(endswith: str) -> Optional[str]:
    for t in tags:
        if t.endswith(endswith) and t.startswith(exp_reactor + ":"):
            return t
    return None

ph_tag = pick_tag(":ph:pH")
do_tag = pick_tag(":do:ppm")

# Temperature: prefer do:oC then ph:oC if only one is available
temp_tag = pick_tag(":do:oC") or pick_tag(":ph:oC")

biomass_tags = sorted([t for t in tags if ":biomass:" in t and t.startswith(exp_reactor + ":")])
default_bio = None
for pref in [":biomass:415", ":biomass:445", ":biomass:480", ":biomass:515", ":biomass:555", ":biomass:590", ":biomass:630", ":biomass:680", ":biomass:nir", ":biomass:clear"]:
    for t in biomass_tags:
        if t.endswith(pref):
            default_bio = t
            break
    if default_bio:
        break
if not default_bio and biomass_tags:
    default_bio = biomass_tags[0]

bio_tag = st.selectbox("Biomass channel", options=biomass_tags or ["(none)"], index=(biomass_tags.index(default_bio) if default_bio in biomass_tags else 0))

plot_tags = [t for t in [ph_tag, do_tag, temp_tag, bio_tag] if t and t != "(none)"]

df_all = db_pg.load_timeseries(sel_id, plot_tags, minutes)

if df_all.empty:
    st.info("No samples in selected window.")
else:
    # Ensure datetime
    if "ts_utc" in df_all.columns:
        df_all["ts_utc"] = pd.to_datetime(df_all["ts_utc"], utc=True, errors="coerce")
        df_all = df_all.dropna(subset=["ts_utc"])

    # Split into 4 panels (scatter)
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)

    def series_df(tag: str) -> pd.DataFrame:
        d = df_all[df_all["tag"] == tag][["ts_utc", "value"]].copy()
        d = d.sort_values("ts_utc")
        return d

    with c1:
        if ph_tag:
            altair_scatter(series_df(ph_tag), f"{exp_reactor} pH (pH)", "pH")
        else:
            st.info("pH tag not found in DB for this experiment.")

    with c2:
        if do_tag:
            altair_scatter(series_df(do_tag), f"{exp_reactor} DO (ppm)", "DO (ppm)")
        else:
            st.info("DO tag not found in DB for this experiment.")

    with c3:
        if temp_tag:
            altair_scatter(series_df(temp_tag), f"{exp_reactor} Temperature (°C)", "Temperature (°C)")
        else:
            st.info("Temperature tag (oC) not found in DB for this experiment.")

    with c4:
        if bio_tag and bio_tag != "(none)":
            altair_scatter(series_df(bio_tag), f"{exp_reactor} Biomass", "Biomass")
        else:
            st.info("Biomass tag not found in DB for this experiment.")

    with st.expander("Raw samples (latest 200)", expanded=False):
        st.dataframe(df_all.tail(200), use_container_width=True, hide_index=True)

st.info("Calibration will only run if the OPC-UA server exposes calibration methods for sensors. If you need a full demo, add calibration methods to mock_server.py.")