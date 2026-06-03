import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import { Kafka } from 'kafkajs';
import { EventEmitter } from 'events';

const fastify = Fastify({ logger: true });
const eventEmitter = new EventEmitter();

await fastify.register(cors);

// --- KAFKA SETUP ---
const kafka = new Kafka({
  clientId: 'tracking-service',
  brokers: (process.env.KAFKA_BROKERS || 'replicationFactor').split(','),
  ssl: false, // Set to true in production if using SASL_SSL
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
});

const consumer = kafka.consumer({ groupId: 'tracking-group' });
const admin = kafka.admin();

async function ensureTopicExists(topic: string) {
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
      });
      console.log(`Tracking Service: Topic "${topic}" created with 3 partitions.`);
    } else {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const currentPartitions = metadata.topics[0].partitions.length;
      if (currentPartitions < 3) {
        await admin.createPartitions({
          topicPartitions: [{ topic, count: 3 }]
        });
        console.log(`Tracking Service: Topic "${topic}" updated to 3 partitions.`);
      }
    }
  } catch (error) {
    console.error(`Tracking Service: Error ensuring topic "${topic}" exists:`, error);
  }
}

async function startKafka() {
  await admin.connect();
  await consumer.connect();

  // Ensure common topics exist so regex subscription works reliably
  const initialTopics = [
    'order.created', 
    'order.accepted', 
    'order.ready', 
    'order.picked_up', 
    'order.delivered'
  ];
  for (const topic of initialTopics) {
    await ensureTopicExists(topic);
  }

  // Subscribe to all order-related topics using a regex
  await consumer.subscribe({ topic: /^order\..*/, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = JSON.parse(message.value!.toString());
      const event = {
        topic,
        payload,
        timestamp: new Date().toISOString()
      };
      console.log(`Tracking Service: Forwarding event ${topic}`);
      eventEmitter.emit('kafka-event', event);
    },
  });
}

// --- SSE ENDPOINT ---
fastify.get('/api/stream', (request, reply) => {
  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache');
  reply.raw.setHeader('Connection', 'keep-alive');
  reply.raw.setHeader('Access-Control-Allow-Origin', '*');

  const onEvent = (event: any) => {
    reply.raw.write(`data: ${JSON.stringify(event)}\n\n`);
  };

  eventEmitter.on('kafka-event', onEvent);

  request.raw.on('close', () => {
    eventEmitter.off('kafka-event', onEvent);
  });
});

const start = async () => {
  try {
    await startKafka();
    const port = parseInt(process.env.PORT || '3004');
    await fastify.listen({ port, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();

// Graceful shutdown
const shutdown = async () => {
  await consumer.disconnect();
  await admin.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
