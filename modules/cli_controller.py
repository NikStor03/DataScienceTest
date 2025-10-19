import sys
import threading

from logger.logger import logger
from modules.replay_engine import ReplayEngine


class CLIController:
    def __init__(self, engine: ReplayEngine):
        self.engine = engine
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        print("Commands: h=historical, l=live, p=pause, r=resume, q=quit")
        while True:
            try:
                cmd = sys.stdin.readline().strip()
                if not cmd:
                    continue
                if cmd == 'h':
                    self.engine.set_mode('historical')
                elif cmd == 'l':
                    self.engine.set_mode('live')
                elif cmd == 'p':
                    self.engine.pause()
                elif cmd == 'r':
                    self.engine.resume()
                elif cmd == 'q':
                    logger.info("Quit requested via CLI")
                    self.engine.stop_all()
                    exit()
                else:
                    logger.warning("Unknown command: %s", cmd)
            except Exception as e:
                logger.error("CLI error: %s", e)
                break
