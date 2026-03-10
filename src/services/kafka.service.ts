import { Kafka, Producer, Consumer, logLevel } from "kafkajs";

let producer: Producer | null = null;
let consumer: Consumer | null = null;

console.log("Initializing KafkaService...");
console.log("Kafka brokers from env:", process.env.KAFKA_BROKERS);
console.log("DATABASE_URL from env:", process.env.DATABASE_URL);
console.log("REDIS_URL from env:", process.env.REDIS_URL);

const kafka = new Kafka({
    clientId: "is-assignment-app",
    brokers: (process.env.KAFKA_BROKERS || "kafka:9092").split(","),
    logLevel: logLevel.ERROR,
    requestTimeout: 30000,
    connectionTimeout: 30000,
});

const initProducer = async (): Promise<Producer> => {
    if (producer) {
        return producer;
    }

    producer = kafka.producer({
        maxInFlightRequests: 5,
        idempotent: true,
        transactionalId: "is-assignment-producer",
        retry: {
            retries: 3,
            initialRetryTime: 300,
            maxRetryTime: 30000,
        },
    });

    try {
        await producer.connect();
        console.log("[KafkaService] Producer connected");
    } catch (error) {
        console.error("[KafkaService] Failed to connect producer:", error);
        throw error;
    }

    return producer;
};

const initConsumer = async (): Promise<Consumer> => {
    if (consumer) {
        return consumer;
    }

    consumer = kafka.consumer({
        groupId: "is-assignment-consumer-group",
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
    });

    try {
        await consumer.connect();
        console.log("[KafkaService] Consumer connected");
    } catch (error) {
        console.error("[KafkaService] Failed to connect consumer:", error);
        throw error;
    }

    return consumer;
};

const publishEvent = async (
    topic: string,
    messages: Array<{ key?: string; value: string }>,
): Promise<void> => {
    try {
        const prod = await initProducer();
        await prod.send({
            topic,
            messages,
        });
        console.log(
            `[KafkaService] Published ${messages.length} message(s) to topic: ${topic}`,
        );
    } catch (error) {
        console.error(
            `[KafkaService] Failed to publish to topic ${topic}:`,
            error,
        );
        throw error;
    }
};

const subscribeToTopic = async (
    topic: string,
    messageHandler: (message: any) => Promise<void>,
    options?: { fromBeginning?: boolean },
): Promise<void> => {
    try {
        const cons = await initConsumer();

        await cons.subscribe({
            topic,
            fromBeginning: options?.fromBeginning || false,
        });

        console.log(`[KafkaService] Subscribed to topic: ${topic}`);

        await cons.run({
            eachMessage: async ({ topic, partition, message }) => {
                const offset = message.offset;
                const key = message.key?.toString() || "none";

                try {
                    console.log(
                        `[KafkaService] Processing message from topic ${topic} partition ${partition} offset ${offset} key ${key}`,
                    );

                    await messageHandler(message);

                    console.log(
                        `[KafkaService] Successfully processed message from offset ${offset}`,
                    );
                } catch (error) {
                    console.error(
                        `[KafkaService] Error processing message at offset ${offset}:`,
                        error,
                    );
                    throw error;
                }
            },
            autoCommit: true,
            autoCommitInterval: 5000,
        });
    } catch (error) {
        console.error("[KafkaService] Failed to subscribe:", error);
        throw error;
    }
};

const disconnect = async (): Promise<void> => {
    try {
        if (producer) {
            await producer.disconnect();
            console.log("[KafkaService] Producer disconnected");
        }
        if (consumer) {
            await consumer.disconnect();
            console.log("[KafkaService] Consumer disconnected");
        }
    } catch (error) {
        console.error("[KafkaService] Error disconnecting:", error);
    }
};

export const KafkaService = {
    initProducer,
    initConsumer,
    publishEvent,
    subscribeToTopic,
    disconnect,
};
