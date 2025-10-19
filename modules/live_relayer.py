import csv
import threading
import time
from datetime import datetime
from typing import Optional

from logger.logger import logger
from modules.queue_manager import QueueManager


class LiveReplayer:

    def __init__(self, queue_mgr: QueueManager, ws_url: Optional[str] = None, simulation_csv: Optional[str] = None, emit_interval: float = 0.01):
        self.queue_mgr = queue_mgr
        self.ws_url = ws_url
        self.simulation_csv = simulation_csv
        self.emit_interval = emit_interval
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

    def start(self):
        self._stop_event.clear()
        threading.Thread(target=self._run_simulation if self.ws_url is None else self._run_ws, name="LiveRunner", daemon=True).start()

    def stop(self):
        self._stop_event.set()

    def pause(self):
        self._pause_event.set()
        logger.info("Live replay paused")

    def resume(self):
        self._pause_event.clear()
        logger.info("Live replay resumed")

    def _run_simulation(self):
        if not self.simulation_csv:
            logger.error("No websocket URL and no simulation CSV provided for live mode")
            return
        logger.debug("Live simulation starts")

        with open(self.simulation_csv, newline='') as f:
            logger.debug("Read file")
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                logger.debug("Index: %d", i)
                if self._stop_event.is_set():
                    break
                self._pause_event.wait()
                payload = dict(row)
                msg = {
                    'mode': 'live',
                    'seq': i,
                    'index': payload['index'],
                    'payload': payload,
                    'received_at': datetime.utcnow().isoformat()
                }
                self.queue_mgr.put(msg)
                logger.debug("Live simulated enqueued seq=%d", i)
                time.sleep(self.emit_interval)
        logger.info("Live simulation finished")

    def _run_ws(self):
        logger.info("Live websocket not implemented in this demo")
