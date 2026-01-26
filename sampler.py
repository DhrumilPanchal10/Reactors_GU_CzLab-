# sampler.py
import asyncio
from datetime import datetime, timezone
import sys
from typing import Any

from client import ReactorOpcClient
from db import ensure_db, create_experiment, insert_sample

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
DEFAULT_REACTOR = "R0"


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


async def main():
    ensure_db()

    reactor = DEFAULT_REACTOR
    exp_id = create_experiment(
        name="Stage2 demo run",
        reactor=reactor,
        started_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    client = ReactorOpcClient(endpoint=ENDPOINT)
    await client.connect()

    print(f"✅ Connected to {ENDPOINT}")
    print(f"✅ Experiment created exp_id={exp_id}")
    print("✅ Starting subscriptions (no polling)")

    # On every subscribed value change: log numeric to DB
    def on_change(nodeid: str, value: Any):
        try:
            # Decide whether it's a sensor or actuator based on client dicts
            info = client.sensor_vars.get(nodeid) or client.actuator_vars.get(nodeid)
            if not info:
                return

            # only log numeric values to samples.value
            if not _is_number(value):
                return

            ts = datetime.now(timezone.utc).isoformat()
            tag = f"{info['reactor']}:{info['name']}:{info['channel']}"
            insert_sample(exp_id, ts, nodeid, tag, float(value))
        except Exception:
            # do not crash callback
            return

    await client.init_subscriptions(on_change=on_change)

    print("⏱️ Sampler running. Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(1.0)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 sampler stopped")