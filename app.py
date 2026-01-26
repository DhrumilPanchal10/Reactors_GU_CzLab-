# app.py
import streamlit as st
from datetime import datetime, timezone
from queue import Queue

from streamlit_autorefresh import st_autorefresh
from variable_map import reactor_map_R0, method_ids_R0
from opc_worker import OpcWorker, Request

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"

st.set_page_config(page_title="Reactors HMI (Stage1+2)", layout="wide")
st.title("Reactors HMI — Stage 1 (live) + Stage 2 (logging)")
st.caption(f"Server endpoint: {ENDPOINT}")

# -----------------------
# Auto-refresh controls (SAFE)
# -----------------------
st.sidebar.header("Auto-refresh")
auto_on = st.sidebar.toggle("Enable auto-refresh", value=False)
auto_interval = st.sidebar.slider(
    "Refresh interval (seconds)",
    min_value=1,
    max_value=10,
    value=2,
)

if auto_on:
    # Causes Streamlit to rerun the script every N seconds
    st_autorefresh(interval=auto_interval * 1000, key="auto_refresh")

# --- start the worker once per session
if "opc_worker" not in st.session_state:
    w = OpcWorker()
    w.start()
    st.session_state["opc_worker"] = w

worker: OpcWorker = st.session_state["opc_worker"]

# --- mappings
varmap = reactor_map_R0()
methods = method_ids_R0()

# --- storage
if "r0_last" not in st.session_state:
    st.session_state["r0_last"] = {}

def rpc_read_all():
    reply_q = Queue()
    req = Request(kind="read_all", endpoint=ENDPOINT, variables=varmap, payload=None, reply_q=reply_q)
    return worker.request(req, timeout=10)

def rpc_write(writes):
    reply_q = Queue()
    req = Request(kind="write", endpoint=ENDPOINT, variables=varmap, payload=writes, reply_q=reply_q)
    return worker.request(req, timeout=10)

def rpc_call(method_nodeid):
    reply_q = Queue()
    req = Request(kind="call", endpoint=ENDPOINT, variables={}, payload=method_nodeid, reply_q=reply_q)
    return worker.request(req, timeout=10)

# --- UI
st.header("R0")

top = st.columns([1, 2, 2])
with top[0]:
    should_refresh = False

if st.button("Refresh R0 (read values)"):
    should_refresh = True

# Auto-refresh also triggers a read
if auto_on:
    should_refresh = True

if should_refresh:
    res = rpc_read_all()
    if res["ok"]:
        st.session_state["r0_last"] = res["data"]
        if not auto_on:
            st.success("Read OK")
    else:
        st.error(f"Read failed: {res['error']}")

with top[1]:
    st.write(f"Last refresh: {datetime.now(timezone.utc).isoformat()}")

with top[2]:
    if st.button("Disconnect all (stop worker)"):
        worker.stop()
        st.session_state.pop("opc_worker", None)
        st.warning("Worker stopped. Reload page to restart.")

st.subheader("Address space (selected variables)")
left, mid, right = st.columns([2, 3, 2])
left.markdown("**NodeId**")
mid.markdown("**Tag**")
right.markdown("**Value**")

last = st.session_state["r0_last"]

for nodeid, info in varmap.items():
    tag = f"{info.reactor}:{info.group}:{info.channel}"
    val = last.get(nodeid, "—")
    left.code(nodeid)
    mid.write(tag)
    right.write(val)

st.divider()

st.subheader("Actuator controls — pwm0")

# find nodeids for pwm0 channels
pwm0_nodes = {info.channel: nodeid for nodeid, info in varmap.items() if info.group == "pwm0"}

if not pwm0_nodes:
    st.error("pwm0 mappings missing in reactor_map_R0()")
else:
    with st.form("pwm0_form"):
        c1, c2 = st.columns(2)
        with c1:
            method_in = st.text_input("R0 pwm0 method", value="PWM")
            time_on_in = st.number_input("R0 pwm0 time_on (s)", value=0.0)
            time_off_in = st.number_input("R0 pwm0 time_off (s)", value=0.0)
        with c2:
            lb_in = st.number_input("R0 pwm0 lb", value=0.0)
            ub_in = st.number_input("R0 pwm0 ub", value=100.0)
            setpoint_in = st.number_input("R0 pwm0 setpoint", value=0.0)

        write_btn = st.form_submit_button("Write pwm0 for R0")

    if write_btn:
        writes = {}
        if "method" in pwm0_nodes:
            writes[pwm0_nodes["method"]] = method_in
        for k, v in [
            ("time_on", float(time_on_in)),
            ("time_off", float(time_off_in)),
            ("lb", float(lb_in)),
            ("ub", float(ub_in)),
            ("setpoint", float(setpoint_in)),
        ]:
            if k in pwm0_nodes:
                writes[pwm0_nodes[k]] = v

        res = rpc_write(writes)
        if res["ok"]:
            st.success("Write OK")
            st.json(res["data"])
        else:
            st.error(f"Write failed: {res['error']}")

st.divider()

st.subheader("Methods")

m1, m2 = st.columns(2)
with m1:
    if st.button("Call set_pairing (R0)"):
        nid = methods.get("set_pairing")
        if not nid:
            st.error("set_pairing NodeId missing in method_ids_R0()")
        else:
            res = rpc_call(nid)
            if res["ok"]:
                st.success(f"set_pairing OK: {res['data']}")
            else:
                st.error(f"set_pairing failed: {res['error']}")

with m2:
    if st.button("Call unpair (R0)"):
        nid = methods.get("unpair")
        if not nid:
            st.error("unpair NodeId missing in method_ids_R0()")
        else:
            res = rpc_call(nid)
            if res["ok"]:
                st.success(f"unpair OK: {res['data']}")
            else:
                st.error(f"unpair failed: {res['error']}")

st.info("R1/R2 will work once you add their mappings in variable_map.py (reactor_map_R1/R2 + method_ids_R1/R2).")