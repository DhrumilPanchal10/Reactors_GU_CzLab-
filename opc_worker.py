# opc_worker.py
import asyncio
import threading
import traceback
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Any, Dict, Optional

from client import ReactorOpcClient


@dataclass
class Request:
    kind: str
    endpoint: str
    variables: Optional[Dict[str, Any]] = None
    payload: Optional[Any] = None
    reply_q: Optional[Queue] = None


class OpcWorker:
    def __init__(self):
        self._req_q: "Queue[Request]" = Queue()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self.client: Optional[ReactorOpcClient] = None
        self.latest_values: Dict[str, Any] = {}
        self.mappings: Dict[str, Any] = {}

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            self._req_q.put_nowait(Request(kind="stop", endpoint=""))
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=5)

    def request(self, req: Request, timeout: float = 20.0):
        if req.reply_q is None:
            req.reply_q = Queue()
        self._req_q.put(req)
        try:
            return req.reply_q.get(timeout=timeout)
        except Empty:
            return {"ok": False, "error": "timeout waiting for worker response"}

    def _run_thread(self):
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._main())
        except Exception:
            traceback.print_exc()

    async def _main(self):
        while not self._stop.is_set():
            req = await self._loop.run_in_executor(None, self._req_q.get)
            if req.kind == "stop":
                await self._shutdown()
                break

            try:
                if req.kind == "connect_browse":
                    res = await self._connect_browse(req.endpoint)
                    self._reply(req, res)
                elif req.kind in ("read_snapshot", "read_all"):
                    self._reply(req, {"ok": True, "data": dict(self.latest_values)})
                elif req.kind == "write":
                    res = await self._write(req.payload or {})
                    self._reply(req, res)
                elif req.kind == "call":
                    res = await self._call(req.payload)
                    self._reply(req, res)
                else:
                    self._reply(req, {"ok": False, "error": f"unknown kind: {req.kind}"})
            except Exception as e:
                self._reply(req, {"ok": False, "error": f"{e}\n{traceback.format_exc()}"})

    def _reply(self, req: Request, res: dict):
        try:
            if req.reply_q:
                req.reply_q.put(res)
        except Exception:
            pass

    async def _shutdown(self):
        try:
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass
        self.client = None

    async def _connect_browse(self, endpoint: str):
        self.client = ReactorOpcClient(endpoint=endpoint)

        def on_change(nodeid: str, value: Any):
            self.latest_values[nodeid] = value

        await self.client.connect()
        self.mappings = await self.client.browse_address_space()
        await self.client.init_subscriptions(on_change_cb=on_change)

        # Prime worker snapshot with current dictionary values
        self.latest_values.update(await self.client.read_snapshot())

        return {"ok": True, "mappings": self.mappings}

    async def _write(self, writes: Dict[str, Any]):
        if not self.client:
            return {"ok": False, "error": "not connected"}
        await self.client.write_bulk(writes)
        self.latest_values.update(writes)
        return {"ok": True, "data": writes}

    async def _call(self, method_nodeid: str):
        if not self.client:
            return {"ok": False, "error": "not connected"}
        out = await self.client.call_method(method_nodeid)
        return {"ok": True, "data": out}
    
    