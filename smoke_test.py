import asyncio
from client import ReactorOpcClient
from variable_map import reactor_map_R0

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"

async def main():
    varmap = reactor_map_R0()
    c = ReactorOpcClient(endpoint=ENDPOINT, variables=varmap)
    await c.connect()
    print("connected")
    v = await c.read("ns=2;i=3")  # R0:ph:pH
    print("read:", v)
    await c.disconnect()
    print("disconnected")

asyncio.run(main())