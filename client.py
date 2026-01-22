# client.py
# ReactorOpcClient - thin async wrapper around asyncua.Client
# Provides:
#   await connect()
#   await disconnect()
#   await read(nodeid_str)
#   await write(nodeid_str, value)
#   await call_method(object_nodeid_str, method_nodeid_str, *args)

import asyncio
import logging
from asyncua import Client, ua

logger = logging.getLogger("ReactorOpcClient")
logging.basicConfig(level=logging.INFO)

class ReactorOpcClient:
    def __init__(self, endpoint: str, timeout: float = 5.0):
        """
        endpoint: opc.tcp://host:port/...
        """
        self.endpoint = endpoint
        self.timeout = timeout
        self._client = None
        self._connected = False

    async def connect(self):
        if self._connected:
            return
        self._client = Client(url=self.endpoint)
        # avoid strict security for mock/dev setups
        try:
            await self._client.connect()
            self._connected = True
            logger.info("Connected to %s", self.endpoint)
        except Exception as e:
            logger.exception("Failed to connect to OPC-UA server: %s", e)
            raise

    async def disconnect(self):
        if self._client and self._connected:
            try:
                await self._client.disconnect()
            except Exception:
                # best-effort
                pass
        self._connected = False
        self._client = None
        logger.info("Disconnected")

    def _ensure_connected(self):
        if not self._connected or self._client is None:
            raise RuntimeError("OPC-UA client is not connected. Call connect() first.")

    async def read(self, nodeid: str):
        """
        Read a Node's value.
        nodeid: string like 'ns=2;i=9' or full expanded NodeId string.
        Returns the Python value (float, int, str, etc.)
        """
        self._ensure_connected()
        try:
            node = self._client.get_node(nodeid)
            val = await node.read_value()
            return val
        except Exception as e:
            logger.exception("Read failed for %s: %s", nodeid, e)
            raise

    async def write(self, nodeid: str, value):
        """
        Write a Python value to a node.
        If the node expects a Variant type, asyncua will attempt conversion.
        """
        self._ensure_connected()
        try:
            node = self._client.get_node(nodeid)
            # attempt to coerce string/number appropriately
            # Node.write_value accepts python primitives; asyncua handles conversion
            await node.write_value(value)
            return True
        except Exception as e:
            logger.exception("Write failed for %s <- %r : %s", nodeid, value, e)
            raise

    async def call_method(self, object_nodeid: str, method_nodeid: str, *args):
        """
        Call a method defined on an object node.
        object_nodeid: NodeId string of the object (e.g. 'ns=2;i=1')
        method_nodeid: NodeId string of the method (e.g. 'ns=2;i=232')
        Returns the method result (may be a Variant or list)
        """
        self._ensure_connected()
        try:
            obj = self._client.get_node(object_nodeid)
            # asyncua's Node.call_method accepts method NodeId (string or NodeId) and args
            res = await obj.call_method(method_nodeid, *args)
            return res
        except Exception as e:
            logger.exception("Method call failed %s.%s : %s", object_nodeid, method_nodeid, e)
            raise

    # Convenience synchronous wrappers (NOT used by your async code, but handy if you want)
    def sync_connect(self):
        return asyncio.get_event_loop().run_until_complete(self.connect())

    def sync_disconnect(self):
        return asyncio.get_event_loop().run_until_complete(self.disconnect())

    def sync_read(self, nodeid):
        return asyncio.get_event_loop().run_until_complete(self.read(nodeid))

    def sync_write(self, nodeid, value):
        return asyncio.get_event_loop().run_until_complete(self.write(nodeid, value))

    def sync_call_method(self, object_nodeid, method_nodeid, *args):
        return asyncio.get_event_loop().run_until_complete(self.call_method(object_nodeid, method_nodeid, *args))

if __name__ == "__main__":
    # quick smoke-test (manual)
    async def _smoke():
        c = ReactorOpcClient("opc.tcp://localhost:4840/freeopcua/server/")
        await c.connect()
        try:
            print("nodes:", await c.read("ns=2;i=9"))
        except Exception as e:
            print("smoke read failed:", e)
        await c.disconnect()

    asyncio.run(_smoke())
