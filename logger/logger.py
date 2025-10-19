import logging
from collections import namedtuple
from typing import Dict, Any

LOG_FORMAT = "%(asctime)s %(levelname)s [%(processName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("replay_engine")


Message = Dict[str, Any]
Checkpoint = namedtuple("Checkpoint", ["last_index", "last_effective_time"])

