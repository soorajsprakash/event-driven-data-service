import "dotenv/config";
import { KafkaService } from "./services/kafka.service";
import { RedisService } from "./services/redis.service";
import pool from "./db";

const TOPIC = "user-events";
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // ms

interface UserEvent {
    event: string;
    timestamp: string;
    data: {
        name: string;
        email: string;
        city: string;
    };
}

// Store processed message offsets to handle duplicates
const processedMessages = new Map<string, string>();

async function processUserEvent(message: Buffer, key: string): Promise<void> {
    const messageId = `${key}-${Date.now()}`;

    // Check for duplicate messages
    if (processedMessages.has(key)) {
        console.log(
            `[Consumer] Duplicate message detected for key: ${key}, skipping...`,
        );
        return;
    }

    try {
        const eventData: UserEvent = JSON.parse(message.toString());

        console.log(
            `[Consumer] Processing event: ${eventData.event} for user ${eventData.data.email}`,
        );

        // Verify data exists in PostgreSQL
        let retries = 0;
        let userExists = false;

        while (retries < MAX_RETRIES && !userExists) {
            try {
                const query = "SELECT id FROM users WHERE email = $1";
                const result = await pool.query(query, [eventData.data.email]);

                if (result.rows.length > 0) {
                    userExists = true;
                    console.log(
                        `[Consumer] User found in database: ${eventData.data.email}`,
                    );
                } else {
                    retries++;
                    if (retries < MAX_RETRIES) {
                        console.log(
                            `[Consumer] User not found in database, retrying... (${retries}/${MAX_RETRIES})`,
                        );
                        await new Promise((resolve) =>
                            setTimeout(resolve, RETRY_DELAY),
                        );
                    } else {
                        throw new Error(
                            `User ${eventData.data.email} not found in database after ${MAX_RETRIES} retries`,
                        );
                    }
                }
            } catch (dbError) {
                retries++;
                if (retries < MAX_RETRIES) {
                    console.log(
                        `[Consumer] Database error, retrying... (${retries}/${MAX_RETRIES})`,
                    );
                    await new Promise((resolve) =>
                        setTimeout(resolve, RETRY_DELAY),
                    );
                } else {
                    throw dbError;
                }
            }
        }

        // Update Redis cache with user data
        const cacheKey = `user:${eventData.data.email}`;
        const cacheData = JSON.stringify(eventData.data);

        await RedisService.set(cacheKey, cacheData, 3600); // Cache for 1 hour

        console.log(
            `[Consumer] Successfully cached user data for ${eventData.data.email}`,
        );

        // Mark message as processed
        processedMessages.set(key, messageId);

        // Keep only last 1000 processed messages to avoid memory leaks
        if (processedMessages.size > 1000) {
            const firstKey = Array.from(processedMessages.keys())[0];
            processedMessages.delete(firstKey);
        }

        console.log(
            `[Consumer] Event processing completed for ${eventData.data.email}`,
        );
    } catch (error) {
        console.error(
            `[Consumer] Failed to process message for key ${key}:`,
            error,
        );
        throw error; // Re-throw to let Kafka handle retry logic
    }
}

async function startConsumer(): Promise<void> {
    try {
        console.log("[Consumer] Starting Kafka consumer...");
        console.log(`[Consumer] Connecting to topic: ${TOPIC}`);

        await KafkaService.subscribeToTopic(TOPIC, async (message) => {
            const key = message.key?.toString() || "unknown";
            await processUserEvent(message.value, key);
        });

        console.log("[Consumer] Consumer started successfully");
    } catch (error) {
        console.error("[Consumer] Fatal error in consumer:", error);
        process.exit(1);
    }
}

// Graceful shutdown
process.on("SIGINT", async () => {
    console.log("[Consumer] Shutting down gracefully...");
    try {
        await KafkaService.disconnect();
        console.log("[Consumer] Consumer disconnected");
    } catch (error) {
        console.error("[Consumer] Error during shutdown:", error);
    }
    process.exit(0);
});

process.on("SIGTERM", async () => {
    console.log("[Consumer] Received SIGTERM, shutting down...");
    try {
        await KafkaService.disconnect();
        console.log("[Consumer] Consumer disconnected");
    } catch (error) {
        console.error("[Consumer] Error during shutdown:", error);
    }
    process.exit(0);
});

// Start the consumer
startConsumer().catch((error) => {
    console.error("[Consumer] Failed to start consumer:", error);
    process.exit(1);
});
