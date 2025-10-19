import argparse
import multiprocessing
import time

from logger.logger import logger
from modules.cli_controller import CLIController
from modules.historical_replayer import HistoricalReplayer
from modules.live_relayer import LiveReplayer
from modules.mid_price_consumer import MidPriceConsumer
from modules.queue_manager import QueueManager
from modules.replay_engine import ReplayEngine, consumer_worker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--historical', required=True, help='Path to historical_sample.csv')
    parser.add_argument('--live', help='Path to live_sample.csv (used to simulate websocket)')
    parser.add_argument('--maxqueue', type=int, default=10000, help='Max queue size')
    parser.add_argument('--consumers', type=int, default=2, help='Number of consumer processes')
    parser.add_argument('--time-scale', type=float, default=1.0, help='Replay time scale (1.0 = real-time)')
    args = parser.parse_args()

    queue_mgr = QueueManager(maxsize=args.maxqueue, spill_dir="./spill")

    hist = HistoricalReplayer(
        args.historical,
        queue_mgr,
        checkpoint_path='hist.checkpoint',
        time_scale=args.time_scale
    )
    live = LiveReplayer(
        queue_mgr,
        ws_url=None,
        simulation_csv=args.live,
        emit_interval=0.001
    )

    engine = ReplayEngine(historical=hist, live=live)

    cli = CLIController(engine)
    cli.start()

    shutdown_event = multiprocessing.Event()
    consumers = []

    for i in range(args.consumers):
        p = multiprocessing.Process(
            target=MidPriceConsumer(
                queue=queue_mgr.queue,
                shutdown_event=shutdown_event,
            ).run,
            name=f"MidPriceConsumer-{i}"
        )
        p.start()
        consumers.append(p)

    engine.set_mode('wait')

    try:
        while True:
            time.sleep(1)
            qsize = queue_mgr.qsize()
            if qsize >= 0:
                logger.debug("Queue size: %d", qsize)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, shutting down")
    finally:
        engine.stop_all()
        shutdown_event.set()
        for p in consumers:
            p.join(timeout=3)
        queue_mgr.close()
        logger.info("Replay engine and consumers terminated")


if __name__ == '__main__':
    main()
