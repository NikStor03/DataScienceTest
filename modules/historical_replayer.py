import csv
import os
import pickle
import threading
import time
from datetime import datetime, timedelta

from logger.logger import logger, Checkpoint
from modules.queue_manager import QueueManager


class HistoricalReplayer:

    def __init__(self, csv_path: str, queue_mgr: QueueManager, checkpoint_path: str = "hist.checkpoint", time_scale: float = 1.0):
        self.csv_path = csv_path
        self.queue_mgr = queue_mgr
        self.checkpoint_path = checkpoint_path
        self.time_scale = time_scale  # allow speeding up replay in testing
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # not paused
        self._rows = []
        self._load_csv()
        self._index = 0
        self._start_wall = None
        self._start_effective = None
        self._lock = threading.Lock()
        self._load_checkpoint()

    def _parse_timestamp(self, s: str) -> datetime:
        fmt = "%Y-%m-%d %H:%M:%S.%f"
        return datetime.strptime(s, fmt)

    def _load_csv(self):
        with open(self.csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_raw = row.get('timestamp') or row.get('time')
                latency_ms = float(row.get('latency_ms') or row.get('latency') or 0)
                ts = self._parse_timestamp(ts_raw)
                effective = ts + timedelta(milliseconds=latency_ms)
                payload = dict(row)
                payload['timestamp'] = ts.isoformat()
                payload['latency_ms'] = latency_ms
                payload['effective'] = effective.isoformat()
                self._rows.append((effective, payload))
        self._rows.sort(key=lambda x: x[0])
        logger.info("Loaded %d historical rows from %s", len(self._rows), self.csv_path)

    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, 'rb') as f:
                    cp = pickle.load(f)
                self._index = cp.last_index + 1
                self._start_effective = cp.last_effective_time
                logger.info("Resuming historical from index %d (last_effective=%s)", self._index, self._start_effective)
            except Exception as e:
                logger.warning("Failed to load checkpoint: %s", e)

    def _save_checkpoint(self, index: int, effective_iso: str):
        try:
            cp = Checkpoint(last_index=index, last_effective_time=effective_iso)
            with open(self.checkpoint_path, 'wb') as f:
                pickle.dump(cp, f)
            logger.debug("Saved checkpoint index=%d, effective=%s", index, effective_iso)
        except Exception as e:
            logger.error("Failed to save checkpoint: %s", e)

    def start(self):
        threading.Thread(target=self._run, name="HistoricalRunner", daemon=True).start()

    def stop(self):
        self._stop_event.set()

    def pause(self):
        self._pause_event.clear()
        logger.info("Historical replay paused")

    def resume(self):
        self._pause_event.set()
        logger.info("Historical replay resumed")

    def _run(self):
        if self._index >= len(self._rows):
            logger.info("No historical data to replay (index >= rows)")
            return
        # compute replay mapping between effective times and wall clock
        now = datetime.utcnow()
        first_effective = self._rows[self._index][0]
        self._start_wall = now
        self._start_effective = first_effective
        logger.info("Historical replay started from index %d (first effective=%s)" , self._index, first_effective.isoformat())

        while not self._stop_event.is_set() and self._index < len(self._rows):
            self._pause_event.wait()
            effective, payload = self._rows[self._index]
            # compute how long to wait in real wall time
            delta_effective = (effective - self._start_effective).total_seconds() / self.time_scale
            target_wall = self._start_wall + timedelta(seconds=delta_effective)
            sleep_seconds = (target_wall - datetime.utcnow()).total_seconds()
            if sleep_seconds > 0:
                time.sleep(min(sleep_seconds, 0.5))
                continue
            # time to emit
            msg = {
                'mode': 'historical',
                'index': self._index,
                'payload': payload,
                'effective': effective.isoformat()
            }
            inserted = self.queue_mgr.put(msg)
            if not inserted:
                logger.debug("Historical message spilled or not enqueued: index=%d", self._index)
            else:
                logger.info("Historical enqueued index=%d effective=%s", self._index, effective.isoformat())
            # checkpoint
            self._save_checkpoint(self._index, effective.isoformat())
            self._index += 1
        logger.info("Historical replay finished or stopped")
