import { Hono } from "hono";
import { connect } from "amqplib";

const app = new Hono();
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://user:password@localhost:5672";
const QUEUE_NAME = process.env.QUEUE_NAME || "csv_processing_queue";
const PORT = 3000;

let channel: any;

async function setupRabbitMQ() {
  try {
    const connection = await connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    console.log(`Connected to RabbitMQ. Queue: ${QUEUE_NAME}`);
  } catch (error) {
    console.error("Failed to connect to RabbitMQ", error);
  }
}

setupRabbitMQ();

app.get("/", (c) => c.text("Ingestor Service Running"));

app.post("/webhook/s3-event", async (c) => {
  if (!channel) {
    return c.json({ error: "Service unavailable (Queue down)" }, 503);
  }

  try {
    const body = await c.req.json();
    
    if (!body.bucket || (!body.key && !body.keys) || !body.pipeline_id) {
      return c.json({ error: "Missing required fields: bucket, key(s), pipeline_id" }, 400);
    }

    const keys = body.keys || [body.key];
    const jobs = keys.map((key: string) => ({
      bucket: body.bucket,
      key,
      pipeline_id: body.pipeline_id
    }));

    for (const job of jobs) {
      const message = JSON.stringify(job);
      channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });
    }
    
    console.log(`[Ingestor] Queued ${jobs.length} jobs`);
    
    return c.json({ status: "queued", count: jobs.length, jobs }, 202);
  } catch (e) {
    return c.json({ error: "Invalid JSON payload" }, 400);
  }
});

export default {
  port: PORT,
  fetch: app.fetch,
};