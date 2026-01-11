# High-Performance CSV ETL Pipeline

This is a high-throughput pipeline designed to ingest, process, and store massive CSV files without breaking a sweat.

### The Stack
*   **Ingestor**: Bun.js + Hono (Super fast HTTP handling)
*   **Queue**: RabbitMQ (Buffers the load so nothing crashes)
*   **Worker**: Python + Polars (Rust-backed data processing, way faster than Pandas)
*   **Storage**: MongoDB (JSON output) + MinIO (S3 compatible storage)

### How It Works
We use the "Claim Check" pattern to keep things flying:

```mermaid
graph LR
    User[User/System] -->|Upload CSV| S3[S3/MinIO]
    User -->|Webhook| Ingestor[Bun Ingestor]
    Ingestor -->|1. Queue Job| RMQ[RabbitMQ]
    RMQ -->|2. Consume| Worker[Python Worker]
    Worker -->|3. Stream Data| S3
    Worker -->|4. Bulk Write| Mongo[MongoDB]
```

1.  **You** upload a file to S3/MinIO.
2.  **Ingestor** gets a webhook, validates it, and drops a "ticket" into RabbitMQ.
3.  **Worker** picks up the ticket, streams the file directly from S3, crunches it with Polars, and bulk inserts it into Mongo.
4.  **Result**: Zero memory bloat, maximum speed.

### Quick Start
No complex setup. Just Docker.

1.  **Spin it up:**
    ```bash
    docker-compose up --build
    ```

2.  **Trigger a job:**
    ```bash
    curl -X POST http://localhost:3000/webhook/s3-event \
        -H "Content-Type: application/json" \
        -d '{
        "bucket": "raw-data",
        "keys": ["file1.csv", "file2.csv", "file3.csv"],
        "pipeline_id": "sales_data"
    }'
    ```

### Configuration
*   Check `config/pipeline_config.json` to define your schemas and transformations.
*   The worker automatically reloads this config on every job.

### Development
*   **Ingestor**: `cd ingestor && bun install && bun dev`
*   **Worker**: `cd worker && uv pip install -r pyproject.toml`
