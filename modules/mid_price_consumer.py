import os
import time
import logging
import multiprocessing as mp
from typing import Dict, Optional

LOG_FORMAT = "%(asctime)s %(levelname)s [%(processName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("mid_price_consumer")

MID_PRICE_FILE = "mid_prices.log"
ERROR_FILE = "errors.log"
DEFAULT_LATENCY_THRESHOLD = 20


class MidPriceConsumer:
    def __init__(
        self,
        queue: mp.Queue,
        shutdown_event: Optional[mp.Event] = None,
        mode: str = "historical",
        latency_threshold: int = DEFAULT_LATENCY_THRESHOLD,
        buffer_size: int = 50
    ):
        self.queue = queue
        self.shutdown_event = shutdown_event or mp.Event()
        self.mode = mode
        self.latency_threshold = latency_threshold
        self.buffer_size = buffer_size
        self.mid_buffer = []
        self.error_buffer = []

        # Ensure output directories exist
        os.makedirs(os.path.dirname(MID_PRICE_FILE) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(ERROR_FILE) or ".", exist_ok=True)

    def process_message(self, msg: Dict):
        """Process a single message from the queue."""
        try:
            timestamp = msg['payload'].get("timestamp")
            bid = msg['payload'].get("bid_price")
            ask = msg['payload'].get("ask_price")
            latency = msg['payload'].get("latency_ms", 0)

            if bid is None or ask is None:
                logger.warning("Skipping message with missing bid/ask: %s", msg)
                return

            if self.mode == "historical" and latency > self.latency_threshold:
                self.error_buffer.append(
                    f"No mid price at {timestamp} as latency {latency}ms "
                    f"is bigger than {self.latency_threshold}ms\n"
                )
            else:
                try:
                    bid = float(bid)
                except ValueError as e:
                    logger.exception("Failed to process message: %s", e)
                    return
                try:
                    ask = float(ask)
                except ValueError as e:
                    logger.exception("Failed to process message: %s", e)
                    return
                mid_price = 0.5 * (bid + ask)
                self.mid_buffer.append(f"{timestamp},{mid_price}\n")

            if len(self.mid_buffer) >= self.buffer_size:
                self.flush_mid_prices()
            if len(self.error_buffer) >= self.buffer_size:
                self.flush_errors()

        except Exception as e:
            logger.exception("Failed to process message: %s", e)

    def flush_mid_prices(self):
        if not self.mid_buffer:
            return
        try:
            with open(MID_PRICE_FILE, "a") as f:
                f.writelines(self.mid_buffer)
            self.mid_buffer.clear()
        except Exception as e:
            logger.exception("Failed to flush mid_prices: %s", e)

    def flush_errors(self):
        if not self.error_buffer:
            return
        try:
            with open(ERROR_FILE, "a") as f:
                f.writelines(self.error_buffer)
            self.error_buffer.clear()
        except Exception as e:
            logger.exception("Failed to flush errors: %s", e)

    def run(self):
        """Main loop: consume messages from queue and process."""
        logger.info("MidPriceConsumer started in mode=%s", self.mode)
        while not self.shutdown_event.is_set():
            try:
                msg = self.queue.get(timeout=1.0)
                if msg:
                    self.process_message(msg)
            except mp.queues.Empty:
                continue
            except KeyboardInterrupt:
                logger.info("Consumer received KeyboardInterrupt")
                break
            except Exception as e:
                logger.exception("Unexpected error in consumer loop: %s", e)

        # Flush remaining buffered messages
        self.flush_mid_prices()
        self.flush_errors()
        logger.info("MidPriceConsumer exiting")
