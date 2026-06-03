import 'dotenv/config';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import { connectDB, Order } from './db.js';
import { connectKafka, produceEvent, disconnectKafka } from './kafka.js';
import { redis } from './redis.js';

const fastify = Fastify({ logger: true });

await fastify.register(cors);

fastify.post('/api/orders', async (request, reply) => {
  const { restaurantId, items, location } = request.body as any;

  // 1. Check cache for restaurant availability (simulated)
  const isAvailable = await redis.get(`restaurant:${restaurantId}:status`);
  if (isAvailable === 'closed') {
    return reply.status(400).send({ error: 'Restaurant is closed' });
  }

  // 2. Save to MongoDB
  const order = new Order({
    restaurantId,
    items,
    location,
    status: 'CREATED'
  });
  await order.save();

  // 3. Produce Kafka Event
  await produceEvent('order.created', {
    orderId: order._id,
    restaurantId,
    items,
    location,
    status: 'CREATED'
  });

  return { orderId: order._id, status: 'CREATED' };
});

const start = async () => {
  try {
    await connectDB();
    await connectKafka();
    const port = parseInt(process.env.PORT || '3000');
    await fastify.listen({ port, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

// Graceful shutdown
const signals: NodeJS.Signals[] = ['SIGTERM', 'SIGINT'];
signals.forEach(sig => {
  process.on(sig, async () => {
    await fastify.close();  
    await disconnectKafka();
    process.exit(0);
  });
});

start();
