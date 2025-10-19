import multiprocessing as mp
import os
import pickle
import queue
import time
from typing import Optional

from logger.logger import Message, logger


class QueueManager:

    def __init__(self, maxsize: int = 10000, spill_dir: str = "./spill"):
        self.maxsize = maxsize
        self.queue = mp.Queue(maxsize=maxsize)
        os.makedirs(spill_dir, exist_ok=True)
        self.spill_dir = spill_dir
        self.spill_counter = mp.Value('i', 0)

    def put(self, item: Message, block: bool = True, timeout: float = 0.5):
        try:
            self.queue.put(item, block=block, timeout=timeout)
            logger.debug("Enqueued message: %s", item.get('index'))
            return True
        except Exception as e:
            # queue full or other issue -> spill to disk
            with self.spill_counter.get_lock():
                idx = self.spill_counter.value
                self.spill_counter.value += 1
            path = os.path.join(self.spill_dir, f"spill_{int(time.time())}_{idx}.pkl")
            try:
                with open(path, 'ab') as f:
                    pickle.dump(item, f)
                logger.warning("Queue full. Spilled message to %s", path)
            except Exception as ex:
                logger.error("Failed to spill message: %s", ex)
            return False

    def get(self, timeout: float = 1.0) -> Optional[Message]:
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def qsize(self) -> int:
        try:
            return self.queue.qsize()
        except Exception:
            return -1

    def close(self):
        try:
            self.queue.close()
        except Exception:
            pass
