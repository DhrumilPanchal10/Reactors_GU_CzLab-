# FINGERPRINT: MOCK_SERVER_STABLE_V3
import asyncio
import random
from asyncua import ua, Server

async def add_writable_var(parent, node_id: ua.NodeId, qname: ua.QualifiedName, value):
    v = await parent.add_variable(node_id, qname, value)
    await v.set_writable()
    return v

async def main():
    server = Server()

    # REQUIRED: initialize standard OPC-UA address space (fixes BadNodeIdUnknown on ServerStatus nodes)
    await server.init()

    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

    # Register ONE namespace; on a fresh server this becomes ns=2
    ns_idx = await server.register_namespace("http://examples.local/reactors/")
    print(f"Namespace index = {ns_idx} (clients expect ns=2)", flush=True)

    # Helper builders
    def nid(i: int) -> ua.NodeId:
        return ua.NodeId(i, ns_idx)

    def qn(name: str) -> ua.QualifiedName:
        # Prevents parsing "R0:ph" as "nsIndex:name"
        return ua.QualifiedName(name, ns_idx)

    await server.start()
    print("✅ Mock OPC-UA server running at opc.tcp://0.0.0.0:4840/freeopcua/server/", flush=True)

    try:
        objects = server.get_objects_node()

        # ---------------------------
        # R0 hierarchy
        # ---------------------------
        r0 = await objects.add_object(nid(1), qn("R0"))

        r0_ph = await r0.add_object(nid(2), qn("R0:ph"))
        await add_writable_var(r0_ph, nid(3), qn("R0:ph:pH"), 7.0)
        await add_writable_var(r0_ph, nid(4), qn("R0:ph:oC"), 25.0)

        r0_do = await r0.add_object(nid(5), qn("R0:do"))
        await add_writable_var(r0_do, nid(6), qn("R0:do:ppm"), 8.0)
        await add_writable_var(r0_do, nid(7), qn("R0:do:oC"), 25.0)

        r0_bio = await r0.add_object(nid(8), qn("R0:biomass"))
        biomass_ids = list(range(9, 19))
        biomass_names = [
            "R0:biomass:415","R0:biomass:445","R0:biomass:480",
            "R0:biomass:515","R0:biomass:555","R0:biomass:590",
            "R0:biomass:630","R0:biomass:680","R0:biomass:clear","R0:biomass:nir"
        ]
        for i, name in zip(biomass_ids, biomass_names):
            await add_writable_var(r0_bio, nid(i), qn(name), 0.0)

        pwm0 = await r0.add_object(nid(19), qn("R0:pwm0"))
        cm = await pwm0.add_object(nid(20), qn("ControlMethod"))

        await add_writable_var(cm, nid(21), qn("value"), 0.0)
        await add_writable_var(pwm0, nid(22), qn("curr_value"), 0.0)
        await add_writable_var(cm, nid(23), qn("method"), "PWM")
        await add_writable_var(cm, nid(24), qn("time_on"), 0.0)
        await add_writable_var(cm, nid(25), qn("time_off"), 0.0)
        await add_writable_var(cm, nid(26), qn("lb"), 0.0)
        await add_writable_var(cm, nid(27), qn("ub"), 100.0)
        await add_writable_var(cm, nid(28), qn("setpoint"), 50.0)
        await add_writable_var(cm, nid(29), qn("reference_sensor"), "R0:biomass:415")

        async def set_pairing(parent, inputs):
            print("🔧 R0 set_pairing called with:", inputs, flush=True)
            return [ua.Variant(True, ua.VariantType.Boolean)]

        await r0.add_method(nid(232), qn("set_pairing"), set_pairing, [], [ua.VariantType.Boolean])

        # ---------------------------
        # Simulation loop
        # ---------------------------
        async def update_loop():
            while True:
                for i in biomass_ids:
                    node = server.get_node(nid(i))
                    val = await node.read_value()
                    await node.write_value(round(val + random.random() * 0.2, 3))

                await server.get_node(nid(3)).write_value(round(7.0 + (random.random() - 0.5) * 0.02, 3))
                await server.get_node(nid(6)).write_value(round(8.0 + (random.random() - 0.5) * 0.1, 3))
                await asyncio.sleep(1)

        asyncio.create_task(update_loop())

        # Keep alive
        while True:
            await asyncio.sleep(1)

    finally:
        await server.stop()
        print("🛑 Server stopped.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
