# client.py
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Any

from asyncua import Client, ua

log = logging.getLogger("ReactorOpcClient")
logging.basicConfig(level=logging.INFO)


@dataclass
class VariableInfo:
    """Metadata stored in ReactorOpcClient.variables (required by spec)."""
    reactor: str            # e.g. "R0"
    kind: str               # "sensor" or "actuator"
    group: str              # e.g. "ph", "do", "biomass", "pwm0"
    channel: str            # e.g. "pH", "ppm", "415", "setpoint"
    nodeid: str             # e.g. "ns=2;i=3"


class ReactorOpcClient:
    """
    Aligns with requirements:
    - Subscription-based updates supported (optional)
    - variables dict maps nodeid -> VariableInfo  [oai_citation:12‡requirements.docx (1).pdf](sediment://file_00000000939471fdbfaa327c84bc2195)
    - write(nodeid, value) exists  [oai_citation:13‡requirements.docx (1).pdf](sediment://file_00000000939471fdbfaa327c84bc2195)
    - provides read() and call_method() helpers
    """
    def __init__(self, endpoint: str, variables: Dict[str, VariableInfo]):
        self.endpoint = endpoint
        self.client: Optional[Client] = None
        self.variables: Dict[str, VariableInfo] = variables  # required contract
        self.cache: Dict[str, Any] = {}  # last known values by nodeid string

    async def connect(self):
        self.client = Client(url=self.endpoint)
        await self.client.connect()
        log.info(f"Connected to {self.endpoint}")

    async def disconnect(self):
        if self.client:
            await self.client.disconnect()
            self.client = None
            log.info("Disconnected")

    def _node(self, nodeid: str):
        if not self.client:
            raise RuntimeError("Client not connected")
        return self.client.get_node(ua.NodeId.from_string(nodeid))

    async def read(self, nodeid: str):
        """Read a node value once."""
        try:
            val = await self._node(nodeid).read_value()
            self.cache[nodeid] = val
            return val
        except Exception as e:
            log.error(f"Read failed for {nodeid}: {e}", exc_info=True)
            raise

    async def write(self, nodeid: str, value_to_write: Any):
        """
        Required by requirements: write(nodeid, value_to_write)  [oai_citation:14‡requirements.docx (1).pdf](sediment://file_00000000939471fdbfaa327c84bc2195)
        """
        try:
            node = self._node(nodeid)
            # Let asyncua infer Variant type for most python primitives
            await node.write_value(value_to_write)
            self.cache[nodeid] = value_to_write
            return True
        except Exception as e:
            log.error(f"Write failed for {nodeid} -> {value_to_write}: {e}", exc_info=True)
            return False

    async def call_method(self, method_nodeid: str, *args):
        """Call OPC-UA method by nodeid (e.g. set_pairing, unpair)."""
        try:
            method_node = self._node(method_nodeid)
            parent = await method_node.get_parent()
            res = await parent.call_method(method_node, *args)
            return res
        except Exception as e:
            log.error(f"Method call failed {method_nodeid}: {e}", exc_info=True)
            return None
        

    