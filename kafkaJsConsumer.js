const { Kafka } = require("kafkajs");

async function main() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["pkc-41p56.asia-south1.gcp.confluent.cloud:9092"],
    ssl: true,
    sasl: {
      mechanism: "PLAIN", // scram-sha-256 or scram-sha-512
      username: "TNYINR7J2YFGTJ3A",
      password:
        "rHWUqMlv0PXf03Rh0oNmdiJ/UdKDSAqXfTwbrJsArkcwkQipw77UbeqKaQ6E6Ih1",
    },
  });

  const consumer = kafka.consumer({ groupId: "test-group" });

  await consumer.connect();
  await consumer.subscribe({ topic: "purchases_", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
}

main();
