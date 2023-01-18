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

  const producer = kafka.producer();

  await producer.connect();
  await producer.send({
    topic: "purchases_",
    messages: [
      { key: "key1", value: "hello world", partition: 0 },
      { key: "key2", value: "hey hey!", partition: 1 },
    ],
  });

  await producer.disconnect();
}

main();
