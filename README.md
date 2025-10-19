# Data Replay & Mid-Price Processor Engine

This project implements a **high-performance data replay engine** capable of processing **historical and live market data**. The engine pushes data into a multiprocessing-safe queue, which is then consumed by a **Mid-Price Processor** to compute and log mid prices.

---

## Table of Contents

- [Features](#features)  
- [Requirements](#requirements)  
- [Installation](#installation)  
- [Project Structure](#project-structure)  
- [Usage](#usage)  
- [Replay Engine Modes](#replay-engine-modes)  
- [Mid-Price Processor](#mid-price-processor)  
- [Configuration](#configuration)  
- [Possible Improvements](#possible-improvements)   

---

## Features

- Unified **Replay Engine** for historical CSV and live data streams.  
- Historical replay respects timestamp + latency ordering.  
- Supports dynamic **mode switching**: `historical` / `live` / `wait`.  
- Multiprocessing-safe **QueueManager** with spill-to-disk when queue is full.  
- **Mid-Price Processor**:
  - Computes `mid_price = 0.5 * (bid + ask)`.  
  - Latency filtering for historical data.  
  - Buffered writes to disk for performance.  
- Modular, testable, and extensible design.  

---

## Requirements

- Python 3.12+  

## Installation

Clone the repository:
```
git clone https://github.com/NikStor03/DataScienceTest.git
cd DataScienceTest
```

## Project Structure

```
.
├── data
│   ├── historical_data.csv
│   └── live_data.csv
├── errors
├── errors.log
├── hist.checkpoint
├── logger
│   ├── logger.cpython-312.pyc
│   └── logger.py
├── main.py                   # Entry point
├── mid_prices.log
├── modules
│   ├── cli_controller.py
│   ├── historical_replayer.py
│   ├── live_relayer.py
│   ├── mid_price_consumer.py
│   ├── queue_manager.py
│   └── replay_engine.py
├── requirements.txt
├── spill
└── task
    └── Python_Engineer_Test.pdf
```

## Usage

Run the main script with the historical CSV:

`python3 main.py --historical data/historical_sample.csv --live data/live_sample.csv --consumers 3 --maxqueue 10000`

Optional arguments:

--consumers: number of MidPriceConsumer processes (default 2).
--maxqueue: maximum queue size (default 10000).
--time-scale: replay speed (1.0 = real-time).

## Replay Engine Modes

You can control the engine **dynamically via CLI**:

| Command | Description |
|---------|-------------|
| `h`     | Switch to `historical` mode |
| `l`     | Switch to `live` mode |
| `p`     | Pause current replay |
| `r`     | Resume paused replay |
| `q`     | Quit application |


## Mid-Price Processor

- Consumes messages from the queue (`QueueManager`).
- Computes **mid-price**:
`mid_price = 0.5 * (bid_price + ask_price)`
- For historical messages, skips messages where:
`latency_ms > LATENCY_THRESHOLD`
- Results written to mid_prices.log
- Errors written to errors.log

## Possible Improvements

- **Priority for spilled messages (FIFO)**:  
  Currently, if the queue is full, ReplayEngine saves new messages into the `./spill` folder to avoid data loss.  
  A potential improvement is to **re-insert these spilled messages into the queue in FIFO order** when space becomes available, giving them **higher priority over newly incoming messages**. This ensures that older messages are processed first, maintaining proper chronological order and reducing potential data inconsistencies.

- **Dynamic latency thresholds**:  
  Allow changing the latency threshold for historical messages at runtime without restarting the consumer processes.

- **Batch processing and async writes**:  
  Further optimize MidPriceConsumer by writing mid prices and errors asynchronously in larger batches to reduce disk I/O overhead.

- **Monitoring and alerting**:  
  Add metrics and alerts for queue saturation, message drop rate, or consumer performance to proactively detect issues in high-throughput environments.

- **WebSocket live data resilience**:  
  Implement reconnection strategies and buffering for live websocket streams to handle network interruptions without losing messages.

