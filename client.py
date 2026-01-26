# client.py
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from asyncua import Client

log = logging.getLogger("ReactorOpcClient")
logging.basicConfig(level=logging.INFO)

METHOD_ENUM = {0: "manual", 1: "timer", 2: "on_boundaries", 3: "pid"}
METHOD_ENUM_INV = {v: k for k, v in METHOD_ENUM.items()}


@dataclass
class VarInfo:
    reactor: str
    name: str          # group name: ph/do/biomass/pwm0...
    channel: str       # pH/ppm/oC/415/.../method/time_on...
    nodeid: str        # "ns=2;i=3"
    value: Any = None


class _SubHandler:
    def __init__(self, on_change: Callable[[str, Any], None]):
        self.on_change = on_change

    def datachange_notification(self, node, val, data):
        try:
            self.on_change(node.nodeid.to_string(), val)
        except Exception:
            pass


class ReactorOpcClient:
    """
    Requirements-compliant client:
    - connect()
    - browse_address_space() -> builds sensor_vars, actuator_vars, methods
    - init_subscriptions(...) -> subscribes to all monitored vars
    - write(nodeid, value) / write_bulk(dict)
    - call_method(nodeid)
    - read_snapshot() -> {nodeid: value}
    """

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.client = Client(url=endpoint)

        # Per requirement: { nodeid_str: {"reactor":"R0","name":"do","channel":"ppm","value":7.8} }
        self.sensor_vars: Dict[str, Dict[str, Any]] = {}
        self.actuator_vars: Dict[str, Dict[str, Any]] = {}
        self.methods: Dict[str, Dict[str, Any]] = {}

        self._subscription = None
        self._sub_handler = None
        self._sub_handles = []
        self._on_change_cb: Optional[Callable[[str, Any], None]] = None

    async def connect(self):
        await self.client.connect()
        log.info("Connected to %s", self.endpoint)
        await self.browse_address_space()
        return True

    async def disconnect(self):
        try:
            if self._subscription:
                try:
                    await self._subscription.delete()
                except Exception:
                    pass
                self._subscription = None
        finally:
            await self.client.disconnect()
            log.info("Disconnected")

    async def browse_address_space(self):
        """
        Automatically discovers reactors (R0,R1,R2) and captures:
          - sensors: ph(pH,oC), do(ppm,oC), biomass(10 chans)
          - actuators: pwm0-3 control vars including method/time_on/time_off/lb/ub/setpoint
          - methods: set_pairing, unpair
        """
        self.sensor_vars.clear()
        self.actuator_vars.clear()
        self.methods.clear()

        objects = self.client.nodes.objects
        kids = await objects.get_children()

        reactors = {}
        for n in kids:
            bn = await n.read_browse_name()
            if bn and bn.Name in {"R0", "R1", "R2"}:
                reactors[bn.Name] = n

        for rname, rnode in reactors.items():
            await self._browse_reactor(rname, rnode)

        log.info(
            "Browse complete. sensors=%d actuators=%d methods=%d",
            len(self.sensor_vars), len(self.actuator_vars), len(self.methods)
        )
        return {
            "sensor_vars": self.sensor_vars,
            "actuator_vars": self.actuator_vars,
            "methods": self.methods,
        }

    async def _browse_reactor(self, reactor: str, reactor_node):
        children = await reactor_node.get_children()

        by_name = {}
        for ch in children:
            bn = await ch.read_browse_name()
            if bn:
                by_name[bn.Name] = ch

        for key, node in by_name.items():
            if key.endswith(":ph"):
                await self._browse_sensor_group(reactor, "ph", node)
            elif key.endswith(":do"):
                await self._browse_sensor_group(reactor, "do", node)
            elif key.endswith(":biomass"):
                await self._browse_biomass(reactor, node)
            elif ":pwm" in key:
                group = key.split(":")[-1]  # pwm0/pwm1/...
                await self._browse_pwm(reactor, group, node)
            elif key in {"set_pairing", "unpair"}:
                nid = node.nodeid.to_string()
                self.methods[nid] = {"reactor": reactor, "name": key, "channel": "", "value": None}

        # Fallback scan for methods (depends on namespace formatting)
        for ch in children:
            bn = await ch.read_browse_name()
            if bn and bn.Name in {"set_pairing", "unpair"}:
                nid = ch.nodeid.to_string()
                self.methods[nid] = {"reactor": reactor, "name": bn.Name, "channel": "", "value": None}

    async def _browse_sensor_group(self, reactor: str, group: str, group_node):
        vars_ = await group_node.get_children()
        for v in vars_:
            bn = await v.read_browse_name()
            if not bn:
                continue
            channel = bn.Name.split(":")[-1]  # pH/ppm/oC
            nid = v.nodeid.to_string()
            try:
                val = await v.read_value()
            except Exception:
                val = None
            self.sensor_vars[nid] = {"reactor": reactor, "name": group, "channel": channel, "value": val}

    async def _browse_biomass(self, reactor: str, group_node):
        vars_ = await group_node.get_children()
        for v in vars_:
            bn = await v.read_browse_name()
            if not bn:
                continue
            channel = bn.Name.split(":")[-1]  # 415/445/.../nir/clear
            nid = v.nodeid.to_string()
            try:
                val = await v.read_value()
            except Exception:
                val = None
            self.sensor_vars[nid] = {"reactor": reactor, "name": "biomass", "channel": channel, "value": val}

    async def _browse_pwm(self, reactor: str, group: str, pwm_node):
        kids = await pwm_node.get_children()

        ctrl = None
        for ch in kids:
            bn = await ch.read_browse_name()
            if bn and bn.Name == "ControlMethod":
                ctrl = ch
                break

        # curr_value directly under pwm node
        for ch in kids:
            bn = await ch.read_browse_name()
            if bn and bn.Name == "curr_value":
                nid = ch.nodeid.to_string()
                try:
                    val = await ch.read_value()
                except Exception:
                    val = None
                self.actuator_vars[nid] = {"reactor": reactor, "name": group, "channel": "curr_value", "value": val}

        if not ctrl:
            return

        ctrl_kids = await ctrl.get_children()
        for v in ctrl_kids:
            bn = await v.read_browse_name()
            if not bn:
                continue
            channel = bn.Name
            if channel == "EnumStrings":
                continue
            nid = v.nodeid.to_string()
            try:
                val = await v.read_value()
            except Exception:
                val = None
            self.actuator_vars[nid] = {"reactor": reactor, "name": group, "channel": channel, "value": val}

    async def init_subscriptions(
        self,
        on_change: Optional[Callable[[str, Any], None]] = None,
        on_change_cb: Optional[Callable[[str, Any], None]] = None,
        **_ignored_kwargs,
    ):
        """
        Subscribes to all sensor + actuator vars.
        Accepts BOTH parameter names to avoid mismatches with callers:
          - on_change
          - on_change_cb
        """
        self._on_change_cb = on_change_cb or on_change

        self._sub_handler = _SubHandler(self._handle_change)
        self._subscription = await self.client.create_subscription(500, self._sub_handler)

        nodeids = list(self.sensor_vars.keys()) + list(self.actuator_vars.keys())
        nodes = [self.client.get_node(nid) for nid in nodeids]

        if nodes:
            handles = await self._subscription.subscribe_data_change(nodes)
            if isinstance(handles, list):
                self._sub_handles.extend(handles)
            else:
                self._sub_handles.append(handles)

        return True

    def _handle_change(self, nodeid: str, value: Any):
        if nodeid in self.sensor_vars:
            self.sensor_vars[nodeid]["value"] = value
        elif nodeid in self.actuator_vars:
            self.actuator_vars[nodeid]["value"] = value

        if self._on_change_cb:
            try:
                self._on_change_cb(nodeid, value)
            except Exception:
                pass

    async def read_snapshot(self) -> Dict[str, Any]:
        snap: Dict[str, Any] = {}
        for nid, info in self.sensor_vars.items():
            snap[nid] = info.get("value")
        for nid, info in self.actuator_vars.items():
            snap[nid] = info.get("value")
        return snap

    async def write(self, nodeid: str, value: Any):
        node = self.client.get_node(nodeid)

        # Allow GUI to pass either label or int for method
        if isinstance(value, str) and value in METHOD_ENUM_INV:
            value = METHOD_ENUM_INV[value]

        await node.write_value(value)

        if nodeid in self.actuator_vars:
            self.actuator_vars[nodeid]["value"] = value
        elif nodeid in self.sensor_vars:
            self.sensor_vars[nodeid]["value"] = value

        return True

    async def write_bulk(self, writes: Dict[str, Any]):
        for nid, val in writes.items():
            await self.write(nid, val)
        return True

    async def call_method(self, method_nodeid: str):
        node = self.client.get_node(method_nodeid)
        parent = await node.get_parent()
        return await parent.call_method(node)