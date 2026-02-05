# sampler.py
import asyncio
import sys
from datetime import datetime, timezone

from client import ReactorOpcClient
import db_pg

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
POLL_DEFAULT = 1.0


def _tag(info: dict) -> str:
    return f"{info.get('reactor','')}:{info.get('name','')}:{info.get('channel','')}".strip(":")


async def main(poll_s: float):
    dbtype = db_pg.ensure_db()
    print(f"[sampler] DB type: {dbtype}")

    client = ReactorOpcClient(endpoint=ENDPOINT)
    await client.connect()

    mappings = await client.browse_address_space()
    sensor_vars = mappings.get("sensor_vars", {}) or {}

    reactors = sorted({info.get("reactor") for info in sensor_vars.values() if isinstance(info, dict) and info.get("reactor")})
    if not reactors:
        reactors = ["R0"]

    exp_ids = {}
    for r in reactors:
        exp_id = db_pg.create_experiment(
            name="Stage2 demo run",
            reactor=r,
            started_at_utc=datetime.now(timezone.utc).isoformat(),
        )
        exp_ids[r] = exp_id
        print(f"✅ Reactor: {r} -> experiment {exp_id}")

    print(f"✅ Sampler connected to {ENDPOINT}")
    print(f"✅ Logging to DB for reactors: {', '.join(reactors)}")
    print(f"⏱️ Interval: {poll_s}s (Ctrl+C to stop)")

    try:
        while True:
            ts = datetime.now(timezone.utc).isoformat()

            for nid, info in sensor_vars.items():
                if not isinstance(info, dict):
                    continue
                reactor = info.get("reactor")
                if reactor not in exp_ids:
                    continue
                try:
                    node = client.client.get_node(nid)
                    v = await node.read_value()
                    if isinstance(v, (int, float)):
                        db_pg.insert_sample(exp_ids[reactor], ts, nid, _tag(info), float(v))
                except Exception:
                    pass

            await asyncio.sleep(poll_s)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    poll = POLL_DEFAULT
    if len(sys.argv) > 1:
        poll = float(sys.argv[1])
    try:
        asyncio.run(main(poll))
    except KeyboardInterrupt:
        print("🛑 sampler stopped")