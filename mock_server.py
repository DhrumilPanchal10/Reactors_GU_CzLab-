# mock_server.py
# FINGERPRINT: MOCK_SERVER_STABLE_V6_LOADED
# Mock OPC-UA server for R0, R1, R2 with calibration methods.
# - Avoids explicit method NodeIds (lets server allocate) to prevent BadNodeIdExists
# - Stable NodeId allocation for main data variables (blocks per reactor)
# - Calibration methods return (status:str, quality:double, cal_value:double)
# - V6: Fixed method signature to use *args (asyncua unpacks InputArguments)

import asyncio
import random
from datetime import datetime, timezone
from asyncua import ua, Server

METHOD_ENUM = {0: "manual", 1: "timer", 2: "on_boundaries", 3: "pid"}


async def add_writable_var(parent, node_id: ua.NodeId, qname: ua.QualifiedName, value):
    v = await parent.add_variable(node_id, qname, value)
    await v.set_writable()
    return v


def _ts():
    return datetime.now(timezone.utc).isoformat()


async def main():
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

    ns_idx = await server.register_namespace("http://examples.local/reactors/")
    print(f"FINGERPRINT: MOCK_SERVER_STABLE_V6_LOADED")
    print(f"Namespace index = {ns_idx} (clients expect ns=2)", flush=True)

    def nid(i: int) -> ua.NodeId:
        # Keep variables in deterministic numerical blocks but do NOT hardcode method ids.
        return ua.NodeId(i, ns_idx)

    def qn(name: str) -> ua.QualifiedName:
        return ua.QualifiedName(name, ns_idx)

    await server.start()
    print("✅ Mock OPC-UA server running at opc.tcp://0.0.0.0:4840/freeopcua/server/", flush=True)

    try:
        objects = server.get_objects_node()

        # NodeId allocation plan:
        # R0: base 1..999
        # R1: base 1001..1999
        # R2: base 2001..2999
        async def build_reactor(reactor: str, base: int):
            r = await objects.add_object(nid(base + 0), qn(reactor))

            # sensors
            ph = await r.add_object(nid(base + 1), qn(f"{reactor}:ph"))
            ph_pH = await add_writable_var(ph, nid(base + 2), qn(f"{reactor}:ph:pH"), 7.0)
            ph_oC = await add_writable_var(ph, nid(base + 3), qn(f"{reactor}:ph:oC"), 25.0)

            do = await r.add_object(nid(base + 4), qn(f"{reactor}:do"))
            do_ppm = await add_writable_var(do, nid(base + 5), qn(f"{reactor}:do:ppm"), 8.0)
            do_oC = await add_writable_var(do, nid(base + 6), qn(f"{reactor}:do:oC"), 25.0)

            bio = await r.add_object(nid(base + 7), qn(f"{reactor}:biomass"))
            biomass_channels = ["415", "445", "480", "515", "555", "590", "630", "680", "clear", "nir"]
            bio_nodes = []
            for idx, ch in enumerate(biomass_channels):
                node = await add_writable_var(
                    bio,
                    nid(base + 8 + idx),
                    qn(f"{reactor}:biomass:{ch}"),
                    0.0,
                )
                bio_nodes.append(node)

            # actuators pwm0..pwm3
            next_id = base + 30
            pwm_nodes = []
            for p in range(4):
                pwm_name = f"{reactor}:pwm{p}"
                pwm = await r.add_object(nid(next_id), qn(pwm_name))
                next_id += 1

                cm = await pwm.add_object(nid(next_id), qn("ControlMethod"))
                next_id += 1

                await add_writable_var(cm, nid(next_id), qn("value"), 0.0); next_id += 1
                await add_writable_var(pwm, nid(next_id), qn("curr_value"), 0.0); next_id += 1
                # method is INT per requirements
                await add_writable_var(cm, nid(next_id), qn("method"), 1); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("time_on"), 0.0); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("time_off"), 0.0); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("lb"), 0.0); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("ub"), 100.0); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("setpoint"), 50.0); next_id += 1
                await add_writable_var(cm, nid(next_id), qn("reference_sensor"), f"{reactor}:biomass:415"); next_id += 1

                pwm_nodes.append((pwm, cm))

            # methods: do NOT pass explicit nodeid -> server will allocate unique ids
            async def set_pairing(parent, inputs):
                print(f"🔧 {reactor} set_pairing called inputs={inputs} ts={_ts()}", flush=True)
                return [ua.Variant(True, ua.VariantType.Boolean)]

            async def unpair(parent, inputs):
                print(f"🔧 {reactor} unpair called inputs={inputs} ts={_ts()}", flush=True)
                return [ua.Variant(True, ua.VariantType.Boolean)]

            # let server pick NodeIds for methods (no nid argument)
            await r.add_method(2, "set_pairing", set_pairing, [], [ua.VariantType.Boolean])
            await r.add_method(2, "unpair", unpair, [], [ua.VariantType.Boolean])

            # calibration stubs: inputs: (double point, double value) -> outputs: (string, double, double)
            # Note: asyncua passes InputArguments as *args, not as a list
            async def make_cal(sensor_id: str):
                async def _cal(parent, *args):
                    try:
                        # args are Variant objects, extract .Value
                        def unwrap(v):
                            return v.Value if hasattr(v, 'Value') else v
                        point = float(unwrap(args[0])) if len(args) > 0 else 0.0
                        value = float(unwrap(args[1])) if len(args) > 1 else 0.0
                    except Exception as e:
                        print(f"⚠️ {reactor} {sensor_id} calibration input error: {e}", flush=True)
                        point, value = 0.0, 0.0
                    status = f"OK:{sensor_id}"
                    quality = 1.0
                    cal_value = value
                    print(f"🧪 {reactor} {sensor_id} calibration point={point} value={value} ts={_ts()}", flush=True)
                    return [
                        ua.Variant(status, ua.VariantType.String),
                        ua.Variant(quality, ua.VariantType.Double),
                        ua.Variant(cal_value, ua.VariantType.Double),
                    ]
                return _cal

            # Add calibration methods (server assigns NodeIds)
            await r.add_method(2, "ph:calibration", await make_cal("ph"),
                               [ua.VariantType.Double, ua.VariantType.Double],
                               [ua.VariantType.String, ua.VariantType.Double, ua.VariantType.Double])

            await r.add_method(2, "do:calibration", await make_cal("do"),
                               [ua.VariantType.Double, ua.VariantType.Double],
                               [ua.VariantType.String, ua.VariantType.Double, ua.VariantType.Double])

            await r.add_method(2, "biomass:calibration", await make_cal("biomass"),
                               [ua.VariantType.Double, ua.VariantType.Double],
                               [ua.VariantType.String, ua.VariantType.Double, ua.VariantType.Double])

            return {
                "ph_pH": ph_pH,
                "ph_oC": ph_oC,
                "do_ppm": do_ppm,
                "do_oC": do_oC,
                "bio_nodes": bio_nodes,
            }

        # Build reactors
        r0 = await build_reactor("R0", 1)
        r1 = await build_reactor("R1", 1001)
        r2 = await build_reactor("R2", 2001)

        # Simulation loop: small random drifts
        async def update_loop():
            while True:
                for r in [r0, r1, r2]:
                    for node in r["bio_nodes"]:
                        v = await node.read_value()
                        await node.write_value(round(float(v) + random.random() * 0.2, 3))

                    pH = await r["ph_pH"].read_value()
                    await r["ph_pH"].write_value(round(float(pH) + (random.random() - 0.5) * 0.02, 3))

                    do = await r["do_ppm"].read_value()
                    await r["do_ppm"].write_value(round(float(do) + (random.random() - 0.5) * 0.1, 3))

                    t1 = await r["ph_oC"].read_value()
                    t2 = await r["do_oC"].read_value()
                    bump = (random.random() - 0.5) * 0.05
                    await r["ph_oC"].write_value(round(float(t1) + bump, 3))
                    await r["do_oC"].write_value(round(float(t2) + bump, 3))

                await asyncio.sleep(1)

        asyncio.create_task(update_loop())

        # keep server running
        while True:
            await asyncio.sleep(1)

    finally:
        await server.stop()
        print("🛑 Server stopped.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())