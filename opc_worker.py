# opc_worker.py
import asyncio
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from queue import Queue, Empty

from client import ReactorOpcClient

@dataclass
class Request:
    kind: str  # "read_all", "write", "call"
    endpoint: str
    variables: Dict[str, Any]  # varmap
    payload: Any
    reply_q: Queue

class OpcWorker:
    """
    Dedicated thread + dedicated asyncio loop.
    Streamlit never runs async; it only sends requests to this worker.
    """
    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._req_q: "Queue[Request]" = Queue()
        self._stop = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        # poke the queue so it can exit
        try:
            self._req_q.put_nowait(Request("stop", "", {}, None, Queue()))
        except Exception:
            pass

    def request(self, req: Request, timeout: float = 10.0):
        self._req_q.put(req)
        try:
            return req.reply_q.get(timeout=timeout)
        except Empty:
            return {"ok": False, "error": "timeout waiting for OPC worker"}

    def _run_thread(self):
        asyncio.run(self._main())

    async def _main(self):
        while not self._stop.is_set():
            # blocking queue read in async loop -> run it in threadpool
            req: Request = await asyncio.get_running_loop().run_in_executor(None, self._req_q.get)

            if req.kind == "stop":
                break

            try:
                if req.kind == "read_all":
                    out = await self._do_read_all(req.endpoint, req.variables)
                    req.reply_q.put({"ok": True, "data": out})
                elif req.kind == "write":
                    out = await self._do_write(req.endpoint, req.variables, req.payload)
                    req.reply_q.put({"ok": True, "data": out})
                elif req.kind == "call":
                    out = await self._do_call(req.endpoint, req.payload)
                    req.reply_q.put({"ok": True, "data": out})
                else:
                    req.reply_q.put({"ok": False, "error": f"unknown request kind: {req.kind}"})
            except Exception as e:
                req.reply_q.put({"ok": False, "error": str(e)})

    async def _do_read_all(self, endpoint: str, varmap: Dict[str, Any]) -> Dict[str, Any]:
        c = ReactorOpcClient(endpoint=endpoint, variables=varmap)
        await c.connect()
        try:
            out = {}
            for nodeid in varmap.keys():
                try:
                    out[nodeid] = await c.read(nodeid)
                except Exception as e:
                    out[nodeid] = f"<ERR: {e}>"
            return out
        finally:
            await c.disconnect()

    async def _do_write(self, endpoint: str, varmap: Dict[str, Any], writes: Dict[str, Any]) -> Dict[str, Any]:
        c = ReactorOpcClient(endpoint=endpoint, variables=varmap)
        await c.connect()
        try:
            out = {}
            for nodeid, value in writes.items():
                try:
                    out[nodeid] = await c.write(nodeid, value)
                except Exception as e:
                    out[nodeid] = f"<ERR: {e}>"
            return out
        finally:
            await c.disconnect()

    async def _do_call(self, endpoint: str, method_nodeid: str):
        c = ReactorOpcClient(endpoint=endpoint, variables={})
        await c.connect()
        try:
            return await c.call_method(method_nodeid)
        finally:
            await c.disconnect()