import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';

// --- DB SETUP ---
const riderSchema = new mongoose.Schema({
  riderId: { type: String, required: true, unique: true },
  isAvailable: { type: Boolean, default: true },
  location: {
    type: { type: String, enum: ['Point'], default: 'Point' },
    coordinates: { type: [Number], required: true } // [longitude, latitude]
  }
});

// CRITICAL: 2dsphere index for geospatial search
riderSchema.index({ location: '2dsphere' });

const Rider = mongoose.model('Rider', riderSchema);

const orderSchema = new mongoose.Schema({ status: String });
const Order = mongoose.model('Order', orderSchema);

async function connectDB() {
  await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/orders');
  console.log('Rider Service: Connected to MongoDB');
}

async function seedRiders() {
  const riders = [
    { riderId: 'rider_1', location: { type: 'Point', coordinates: [-74.0060, 40.7128] }, isAvailable: true },
    { riderId: 'rider_2', location: { type: 'Point', coordinates: [-74.0050, 40.7138] }, isAvailable: true },
    { riderId: 'rider_3', location: { type: 'Point', coordinates: [-74.0040, 40.7148] }, isAvailable: true },
  ];

  for (const rider of riders) {
    await Rider.findOneAndUpdate({ riderId: rider.riderId }, rider, { upsert: true });
  }
  console.log('Rider Service: Seeded riders in NYC area');
}

// --- KAFKA SETUP ---
const kafka = new Kafka({
  clientId: 'rider-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9098,localhost:9095,localhost:9096').split(','),
  ssl: false, // Set to true in production if using SASL_SSL
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'rider-group' });
const admin = kafka.admin();

async function ensureTopicExists(topic: string) {
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
      });
      console.log(`Rider Service: Topic "${topic}" created with 3 partitions.`);
    } else {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const currentPartitions = metadata.topics[0].partitions.length;
      if (currentPartitions < 3) {
        await admin.createPartitions({
          topicPartitions: [{ topic, count: 3 }]
        });
        console.log(`Rider Service: Topic "${topic}" updated to 3 partitions.`);
      }
    }
  } catch (error) {
    console.error(`Rider Service: Error ensuring topic "${topic}" exists:`, error);
  }
}

// --- FASTIFY SETUP ---
const fastify = Fastify({ logger: true });
await fastify.register(cors);

fastify.post('/api/riders/update-location', async (request, reply) => {
  const { riderId, location } = request.body as any; // location: [lng, lat]
  
  await Rider.findOneAndUpdate(
    { riderId },
    { location: { type: 'Point', coordinates: location }, isAvailable: true },
    { upsert: true }
  );

  return { status: 'Location Updated' };
});

async function start() {
  await connectDB();
  await seedRiders();
  await admin.connect();
  await producer.connect();
  await consumer.connect();

  await ensureTopicExists('order.ready');
  await ensureTopicExists('order.picked_up');
  await ensureTopicExists('order.delivered');

  await consumer.subscribe({ topic: 'order.ready', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value!.toString());
      console.log(`Rider Service: Finding rider for order ${event.orderId}`);

      const { lat, lng } = event.location;

      // GEOSPATIAL SEARCH: Find nearest available rider within 5km
      const nearestRider = await Rider.findOne({
        isAvailable: true,
        location: {
          $near: {
            $geometry: { type: 'Point', coordinates: [lng, lat] },
            $maxDistance: 5000 
          }
        }
      });

      if (!nearestRider) {
        console.log(`Rider Service: No rider available for order ${event.orderId}`);
        return;
      }

      console.log(`Rider Service: Assigned rider ${nearestRider.riderId} to order ${event.orderId}`);

      // 1. Mark rider as unavailable
      nearestRider.isAvailable = false;
      await nearestRider.save();

      // 2. Update Order Status
      await Order.findByIdAndUpdate(event.orderId, { status: 'PICKED_UP' });

      // 3. Emit Kafka Events
      await producer.send({
        topic: 'order.picked_up',
        messages: [{ value: JSON.stringify({ orderId: event.orderId, riderId: nearestRider.riderId, status: 'PICKED_UP' }) }]
      });

      // 4. Simulate Delivery (5 seconds later)
      setTimeout(async () => {
        await Order.findByIdAndUpdate(event.orderId, { status: 'DELIVERED' });
        await Rider.findOneAndUpdate({ riderId: nearestRider.riderId }, { isAvailable: true });
        
        await producer.send({
          topic: 'order.delivered',
          messages: [{ value: JSON.stringify({ orderId: event.orderId, status: 'DELIVERED' }) }]
        });
        console.log(`Rider Service: Order ${event.orderId} DELIVERED`);
      }, 5000);
    },
  });

  const port = parseInt(process.env.PORT || '3002');
  await fastify.listen({ port, host: '0.0.0.0' });
}

start().catch(console.error);

const shutdown = async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await admin.disconnect();
  await fastify.close();
  await mongoose.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
