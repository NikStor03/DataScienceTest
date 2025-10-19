import multiprocessing as mp
import threading
import time
from typing import Optional

from logger.logger import logger
from modules.historical_replayer import HistoricalReplayer
from modules.live_relayer import LiveReplayer
from modules.queue_manager import QueueManager


class ReplayEngine:
    def __init__(self, historical: Optional[HistoricalReplayer], live: Optional[LiveReplayer]):
        self.historical = historical
        self.live = live
        self.mode = None
        self._lock = threading.Lock()

    def set_mode(self, mode: str):
        with self._lock:
            if mode == 'historical':
                logger.info("Switching engine to HISTORICAL mode")
                # stop live if running
                if self.live:
                    self.live.stop()
                # start or resume historical
                if self.historical:
                    self.historical.start()
                self.mode = 'historical'
            elif mode == 'live':
                logger.info("Switching engine to LIVE mode")
                # stop historical
                if self.historical:
                    self.historical.stop()
                if self.live:
                    self.live.start()
                self.mode = 'live'
            elif mode == 'wait':
                logger.info("Switching engine to WAIT mode")
            else:
                logger.error("Unknown mode: %s", mode)

    def pause(self):
        with self._lock:
            if self.mode == 'historical' and self.historical:
                self.historical.pause()
            elif self.mode == 'live' and self.live:
                self.live.pause()

    def resume(self):
        with self._lock:
            if self.mode == 'historical' and self.historical:
                self.historical.resume()
            elif self.mode == 'live' and self.live:
                self.live.resume()

    def stop_all(self):
        if self.historical:
            self.historical.stop()
        if self.live:
            self.live.stop()


def consumer_worker(queue_mgr: QueueManager, consumer_id: int, shutdown_event: mp.Event):
    proc_name = mp.current_process().name
    logger.info("Consumer %d started (%s)", consumer_id, proc_name)
    while not shutdown_event.is_set():
        msg = queue_mgr.get(timeout=1.0)
        if msg is None:
            continue
        logger.info("Consumer %d processed message mode=%s meta=%s", consumer_id, msg.get('mode'), {k: msg.get('index') or msg.get('seq')})
        time.sleep(0.001)
    logger.info("Consumer %d shutting down", consumer_id)
