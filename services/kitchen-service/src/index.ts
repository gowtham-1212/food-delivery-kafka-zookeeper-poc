import 'dotenv/config';
import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';

// --- DB SETUP ---
const orderSchema = new mongoose.Schema({
  restaurantId: String,
  items: [String],
  status: String,
  updatedAt: { type: Date, default: Date.now }
});
const Order = mongoose.model('Order', orderSchema);

async function connectDB() {
  await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/orders');
  console.log('Kitchen Service: Connected to MongoDB');
}

// --- KAFKA SETUP ---
const kafka = new Kafka({
  clientId: 'kitchen-service',
  brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9098,127.0.0.1:9095,127.0.0.1:9096').split(','),
  ssl: false, // Set to true in production if using SASL_SSL
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'kitchen-group' });
const admin = kafka.admin();

async function ensureTopicExists(topic: string) {
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
      });
      console.log(`Kitchen Service: Topic "${topic}" created with 3 partitions.`);
    } else {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const currentPartitions = metadata.topics[0].partitions.length;
      if (currentPartitions < 3) {
        await admin.createPartitions({
          topicPartitions: [{ topic, count: 3 }]
        });
        console.log(`Kitchen Service: Topic "${topic}" updated to 3 partitions.`);
      }
    }
  } catch (error) {
    console.error(`Kitchen Service: Error ensuring topic "${topic}" exists:`, error);
  }
}

async function start() {
  await connectDB();
  await admin.connect();
  await producer.connect();
  await consumer.connect();

  await ensureTopicExists('order.created');
  await ensureTopicExists('order.accepted');
  await ensureTopicExists('order.ready');

  await consumer.subscribe({ topic: 'order.created', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value!.toString());
      console.log(`Kitchen Service: Received order.created for ${event.orderId}`);

      // 1. Update status to ACCEPTED
      await Order.findByIdAndUpdate(event.orderId, { status: 'ACCEPTED' });
      await producer.send({
        topic: 'order.accepted',
        messages: [{ value: JSON.stringify({ orderId: event.orderId, status: 'ACCEPTED' }) }]
      });

      // 2. Simulate food preparation (5 seconds)
      setTimeout(async () => {
        await Order.findByIdAndUpdate(event.orderId, { status: 'READY' });
        await producer.send({
          topic: 'order.ready',
          messages: [{ value: JSON.stringify({ 
            orderId: event.orderId, 
            status: 'READY',
            location: event.location 
          }) }]
        });
        console.log(`Kitchen Service: Order ${event.orderId} is READY`);
      }, 5000);
    },
  });
}

start().catch(console.error);

// Graceful shutdown
const shutdown = async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await admin.disconnect();
  await mongoose.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
