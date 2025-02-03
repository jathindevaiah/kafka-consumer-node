import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import axios from "axios";

dotenv.config();

// Set up Kafka configuration
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID, // Replace with your app name
  brokers: [process.env.KAFKA_BROKER], // Replace with your Confluent Cloud broker(s)
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY, // Replace with your API key
    password: process.env.KAFKA_API_SECRET, // Replace with your API secret
  },
});

// Create consumer
const consumer = kafka.consumer({ groupId: "test-group" });

// Confluent Schema Registry URL
const registry = new SchemaRegistry({
  host: process.env.KAFKA_SR_URL, // Schema Registry URL
  auth: {
    username: process.env.KAFKA_SR_API_KEY, // API key
    password: process.env.KAFKA_SR_API_SECRET, // API secret
  },
});

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true });

  console.log(`Consumer connected to topic: ${process.env.KAFKA_TOPIC}`);

  // Consume messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // console.log(`Received message: ${message.value.toString()}`);
      try {
        // Deserialize the message value using the schema registry (Avro decoding)
        const decodedMessage = await registry.decode(message.value);

        // Process the decoded message
        console.log(`Received message from ${topic}'s partition ${partition}:`, decodedMessage.toString());

        // Example: Accessing fields from the decoded message
        // const { user_id, name, email, created_at } = decodedMessage;
        // console.log(`User ID: ${user_id}, Name: ${name}, Email: ${email}, Created At: ${created_at}`);
      } catch (error) {
        console.error("Error deserializing message:", error);
      }
    },
  });
};

run().catch(console.error);
