# Streaming Store Analytics

A beginner-friendly, end-to-end streaming data engineering project. It generates synthetic online-store activity, sends events through Kafka-compatible Redpanda, and uses Spark Structured Streaming to calculate 30-second event-time aggregates that are stored as Parquet.

## What you will build

```text
Synthetic event producer  ->  Redpanda (Kafka API)  ->  Spark Structured Streaming
                                                           |
                                                           v
                                              Parquet window aggregates
```

Each event includes an ID, UTC event timestamp, user, product, event type, region, and purchase amount. The Spark job groups events by 30-second event-time window, region, and event type, then records event counts and revenue. A 10-second watermark bounds state while allowing a small amount of late-arriving data.

## Prerequisites

- Docker Desktop with Docker Compose
- Python 3.10 or 3.11
- Java 17 (required by Spark)

## Run locally (Windows PowerShell)

From this project folder:

1. Start Redpanda and its browser console:

   ```powershell
   docker compose up -d
   ```

   The Kafka API is available at `localhost:19092`; the Redpanda Console is at <http://localhost:8080>.

2. Create a virtual environment and install dependencies:

   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   python -m pip install -r requirements.txt
   ```

3. Start the Spark streaming job in one terminal. The first run downloads the Spark Kafka connector package:

   ```powershell
   spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/stream_processor.py
   ```

4. In a second terminal, activate the same virtual environment and publish synthetic events:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   python -m src.event_producer --events 300 --interval-seconds 0.2
   ```

   By default the processor reads only newly published events. To replay existing topic data, stop Spark and restart it with `KAFKA_STARTING_OFFSETS=earliest` set in that terminal:

   ```powershell
   $env:KAFKA_STARTING_OFFSETS = "earliest"
   spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/stream_processor.py
   ```

5. Let the processor run until windows close and aggregates are written, then stop it with `Ctrl+C`. Results are in `data/aggregates/`. The checkpoint at `data/checkpoints/store-analytics/` tracks stream progress; keep it to resume the same query. For a clean replay, stop Spark and remove both output and checkpoint directories before restarting.

To stop Redpanda and the console:

```powershell
docker compose down
```

## Run the unit tests

```powershell
python -m unittest discover -s tests -v
```

## Configuration

The defaults are suitable for a local run. Override them with environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka API address |
| `KAFKA_TOPIC` | `store-events` | Input topic |
| `KAFKA_STARTING_OFFSETS` | `latest` | Initial Kafka offset (`latest` or `earliest`) |
| `OUTPUT_PATH` | `data/aggregates` | Parquet output directory |
| `CHECKPOINT_PATH` | `data/checkpoints/store-analytics` | Spark checkpoint directory |

The producer also accepts `--events`, `--interval-seconds`, and `--seed`; run `python -m src.event_producer --help` for details.

## Inspect the Parquet output

After the stream has written at least one closed window, use PySpark to inspect its results:

```powershell
pyspark
```

```python
spark.read.parquet("data/aggregates").orderBy("window_start").show(truncate=False)
```

Each output row contains `window_start`, `window_end`, `region`, `event_type`, `event_count`, and `revenue`. Revenue is zero for views and add-to-cart events.

## Troubleshooting

- **Spark cannot connect to Kafka:** Check that `docker compose ps` reports Redpanda as healthy and that Spark uses `localhost:19092`.
- **Class not found for Kafka source:** Keep the connector's Spark/Scala versions aligned with PySpark 3.5.1: `spark-sql-kafka-0-10_2.12:3.5.1`.
- **No Parquet rows yet:** Aggregates are emitted in append mode only after the watermark closes a window. Keep producing events long enough to advance event time and wait for a subsequent Spark micro-batch.
- **A changed query cannot reuse its checkpoint:** Stop the processor and clear both `data/checkpoints/store-analytics/` and `data/aggregates/` before starting a fresh run.
