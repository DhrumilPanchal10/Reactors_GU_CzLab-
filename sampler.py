# sampler.py
"""
Sampler service for Stage 2:
- Connects to OPC-UA endpoint
- Reads configured nodeids for each reactor every interval_s seconds
- Inserts timestamped rows into SQLite via db.insert_samples(rows)

How to run:
    source .venv/bin/activate
    python sampler.py 1    # sample every 1 second
"""

import asyncio
import datetime
import sys
import time
from client import ReactorOpcClient
import db

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"

# === CONFIG: NodeId mapping per reactor ===
# R0 nodeids come from the Requirements PDF:
# Biomass channels: ns=2;i=9..18 (415..nir).  [oai_citation:4‡requirements.docx.pdf](file-service://file-25hQ5ubh5ZwN6ZyU8qKhnB)
# pwm0 ControlMethod variables: method=ns=2;i=23, time_on=24, time_off=25, lb=26, ub=27, setpoint=28.  [oai_citation:5‡requirements.docx.pdf](file-service://file-25hQ5ubh5ZwN6ZyU8qKhnB)
#
# IMPORTANT: Update R1/R2 nodeids to the real values when you confirm them.
REACTORS = {
    "R0": {
        # biomass wavelengths mapping (tag -> nodeid)
        "biomass_415": "ns=2;i=9",
        "biomass_445": "ns=2;i=10",
        "biomass_480": "ns=2;i=11",
        "biomass_515": "ns=2;i=12",
        "biomass_555": "ns=2;i=13",
        "biomass_590": "ns=2;i=14",
        "biomass_630": "ns=2;i=15",
        "biomass_680": "ns=2;i=16",
        "biomass_clear": "ns=2;i=17",
        "biomass_nir": "ns=2;i=18",
        # DO / pH (kept for completeness)
        "ph_pH": "ns=2;i=3",
        "do_ppm": "ns=2;i=6",
        # pwm0 control fields (ControlMethod under pwm0)
        "pwm0_method": "ns=2;i=23",
        "pwm0_time_on": "ns=2;i=24",
        "pwm0_time_off": "ns=2;i=25",
        "pwm0_lb": "ns=2;i=26",
        "pwm0_ub": "ns=2;i=27",
        "pwm0_setpoint": "ns=2;i=28",
    },

    # Placeholder entries for R1 / R2. Replace these nodeids with the real ones when available.
}

# flatten list of tags we want to log per reactor (biomass list first, then control fields)
BIOMASS_TAGS = [
    "biomass_415","biomass_445","biomass_480","biomass_515","biomass_555",
    "biomass_590","biomass_630","biomass_680","biomass_clear","biomass_nir"
]

CONTROL_TAGS = [
    "pwm0_method","pwm0_time_on","pwm0_time_off","pwm0_lb","pwm0_ub","pwm0_setpoint"
]

# Combined tagging order
LOG_TAGS = BIOMASS_TAGS + ["ph_pH","do_ppm"] + CONTROL_TAGS

async def main(interval_s: float = 1.0):
    db.init_db()

    client = ReactorOpcClient(ENDPOINT)
    await client.connect()
    print(f"✅ Sampler connected to {ENDPOINT}")
    print(f"✅ Logging to {db.DB_PATH}")
    print(f"⏱️ Interval: {interval_s}s (Ctrl+C to stop)")

    try:
        while True:
            ts = datetime.datetime.utcnow().isoformat()
            rows = []

            for reactor_name, mapping in REACTORS.items():
                for tag in LOG_TAGS:
                    nodeid = mapping.get(tag)
                    if not nodeid:
                        # skip tags not configured for this reactor
                        continue
                    try:
                        val = await client.read(nodeid)
                        # coerce to float where possible; skip non-numeric values for DB
                        try:
                            valf = float(val)
                        except Exception:
                            # if the value is a string representing a mode (method), handle as NaN or skip
                            # We will attempt to store numeric values only; if you want to store enums,
                            # extend the schema to include text values.
                            # For now store NaN as None (skip).
                            print(f"[INFO] Non-numeric value for {reactor_name}:{tag} -> {val}", file=sys.stderr)
                            continue
                        rows.append((ts, reactor_name, tag, nodeid, valf))
                    except Exception as e:
                        print(f"[WARN] read failed {reactor_name}:{tag} ({nodeid}): {e}", file=sys.stderr)

            # Bulk insert
            if rows:
                try:
                    db.insert_samples(rows)
                    print(f"{ts} wrote {len(rows)} samples")
                except Exception as e:
                    print(f"[ERROR] DB insert failed: {e}", file=sys.stderr)

            await asyncio.sleep(interval_s)

    except KeyboardInterrupt:
        print("🛑 Sampler stopped by user")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    interval = 1.0
    if len(sys.argv) > 1:
        try:
            interval = float(sys.argv[1])
        except Exception:
            pass
    asyncio.run(main(interval))