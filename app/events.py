import queue
import threading
import time

from . import store

_bus: dict[str, queue.Queue] = {}
_lock = threading.Lock()


def get_queue(session_id: str) -> queue.Queue:
    with _lock:
        if session_id not in _bus:
            _bus[session_id] = queue.Queue()
        return _bus[session_id]


def emit(session_id: str, stage: str, status: str, message: str,
         progress: float | None = None, eta: float | None = None, topic: str | None = None):
    ev = {
        "ts": time.time(),
        "stage": stage,
        "status": status,
        "message": message,
        "progress": progress,
        "eta": eta,
        "topic": topic,
    }
    try:
        store.log_event(session_id, stage, status, message, progress, eta, topic)
    except Exception:
        pass
    get_queue(session_id).put(ev)
