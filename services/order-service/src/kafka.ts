import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9098,127.0.0.1:9095,127.0.0.1:9096').split(','),
  ssl: false, // Set to true in production if using SASL_SSL
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
});

const producer = kafka.producer();
const admin = kafka.admin();

export async function connectKafka() {
  await producer.connect();
  await admin.connect();
  console.log('Connected to Kafka');
  await ensureTopicExists('order.created');
}

async function ensureTopicExists(topic: string) {
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
      });
      console.log(`Topic "${topic}" created with 3 partitions.`);
    } else {
      // If it exists, check partition count and increase if necessary
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const currentPartitions = metadata.topics[0].partitions.length;
      if (currentPartitions < 3) {
        await admin.createPartitions({
          topicPartitions: [{ topic, count: 3 }]
        });
        console.log(`Topic "${topic}" updated to 3 partitions (was ${currentPartitions}).`);
      } else {
        console.log(`Topic "${topic}" already has ${currentPartitions} partitions.`);
      }
    }
  } catch (error) {
    console.error(`Error ensuring topic "${topic}" exists:`, error);
  }
}

export async function produceEvent(topic: string, message: any) {
  await ensureTopicExists(topic);
  await producer.send({
    topic,
    messages: [
      { value: JSON.stringify(message) }
    ]
  });
}

export async function disconnectKafka() {
  await producer.disconnect();
  await admin.disconnect();
}
