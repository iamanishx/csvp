import os
import json
import time
import pika
import polars as pl
from pymongo import MongoClient
import s3fs

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://user:password@localhost:5672")
QUEUE_NAME = os.getenv("QUEUE_NAME", "csv_processing_queue")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:password@localhost:27017")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/pipeline_config.json")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def get_pipeline_config(config, pipeline_id):
    for p in config["pipelines"]:
        if p["pipeline_id"] == pipeline_id:
            return p
    return None


def process_file(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received job: {message}")

    bucket = message["bucket"]
    key = message["key"]
    pipeline_id = message["pipeline_id"]

    config = load_config()
    pipeline_conf = get_pipeline_config(config, pipeline_id)

    if not pipeline_conf:
        print(f"Error: Pipeline ID {pipeline_id} not found in config")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    s3_path = f"s3://{bucket}/{key}"

    storage_options = {
        "endpoint_url": MINIO_ENDPOINT,
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "use_ssl": False,
    }

    try:
        print(f"Reading from {s3_path}...")
        df = pl.read_csv(s3_path, storage_options=storage_options)

        transforms = pipeline_conf.get("transformations", {})

        if "rename_columns" in transforms:
            df = df.rename(transforms["rename_columns"])

        if "drop_columns" in transforms:
            cols_to_drop = [c for c in transforms["drop_columns"] if c in df.columns]
            if cols_to_drop:
                df = df.drop(cols_to_drop)

        records = df.to_dicts()

        if records:
            client = MongoClient(MONGO_URI)
            db = client["csv_pipeline"]
            collection = db[pipeline_conf["mongo_collection"]]

            collection.insert_many(records)
            print(
                f"Inserted {len(records)} records into {pipeline_conf['mongo_collection']}"
            )
            client.close()
        else:
            print("No records to insert.")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing file: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    print("Worker starting...")
    params = pika.URLParameters(RABBITMQ_URL)
    connection = None

    while True:
        try:
            connection = pika.BlockingConnection(params)
            break
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ...")
            time.sleep(5)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_file)

    print("Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
