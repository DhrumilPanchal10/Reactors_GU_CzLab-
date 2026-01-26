# sampler.py
import asyncio
from datetime import datetime, timezone
import sys
import sqlite3

from client import ReactorOpcClient
from variable_map import reactor_map_R0
import db

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"

POLL_DEFAULT = 1.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main(poll_s: float):
    # 1) Ensure DB + create an experiment row
    db.ensure_db()
    exp_id = db.create_experiment(
        name="Stage2 demo run",
        reactor="R0",
        started_at_utc=utc_now_iso(),
    )

    # 2) Build variable map + connect client
    varmap = reactor_map_R0()
    client = ReactorOpcClient(endpoint=ENDPOINT, variables=varmap)

    await client.connect()
    print(f"✅ Sampler connected to {ENDPOINT}")
    print(f"✅ Logging to {db.DB_PATH} (experiment_id={exp_id})")
    print(f"⏱️ Interval: {poll_s}s (Ctrl+C to stop)")

    # 3) Main loop: read all nodes, write once per cycle
    try:
        while True:
            ts = utc_now_iso()

            rows = []
            for nodeid, info in varmap.items():
                try:
                    v = await client.read(nodeid)

                    # Only store numeric values in samples.value (REAL)
                    if isinstance(v, (int, float)):
                        tag = f"{info.reactor}:{info.group}:{info.channel}"
                        rows.append((exp_id, ts, nodeid, tag, float(v)))
                    else:
                        # Example: method="PWM" -> skip (not numeric)
                        continue

                except Exception as e:
                    # One-line warning, no stack spam
                    print(f"[WARN] read failed {info.reactor}:{info.group}:{info.channel} {nodeid}: {e}")

            # 4) Bulk insert rows in a single transaction (FAST)
            if rows:
                try:
                    with sqlite3.connect(db.DB_PATH) as con:
                        cur = con.cursor()
                        cur.executemany(
                            "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (?, ?, ?, ?, ?)",
                            rows,
                        )
                        con.commit()
                    print(f"[OK] {ts} inserted {len(rows)} samples")
                except Exception as e:
                    print(f"[WARN] DB write failed: {e}")

            await asyncio.sleep(poll_s)

    finally:
        await client.disconnect()
        print("🛑 sampler stopped")


if __name__ == "__main__":
    poll = POLL_DEFAULT
    if len(sys.argv) > 1:
        poll = float(sys.argv[1])

    try:
        asyncio.run(main(poll))
    except KeyboardInterrupt:
        pass

