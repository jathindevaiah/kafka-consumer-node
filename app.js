import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

// Set up Kafka configuration
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID, // Replace with your app name
  brokers: [process.env.KAFKA_BROKER], // Replace with your Confluent Cloud broker(s)
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_API_KEY, // Replace with your API key
    password: process.env.KAFKA_API_SECRET, // Replace with your API secret
  },
});

// Create consumer
const consumer = kafka.consumer({ groupId: 'test-group' }); // Replace with your consumer group ID

const run = async () => {
  await consumer.connect();

  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true }); // Replace with your topic name

  // Consume messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`);
    },
  });
};

run().catch(console.error);
