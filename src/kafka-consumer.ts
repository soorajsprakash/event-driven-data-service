import "dotenv/config";
import { KafkaService } from "./services/kafka.service";
import { RedisService } from "./services/redis.service";
import { UserEvent, UserRow } from "./models/data.response";

const TOPIC = process.env.KAFKA_TOPIC;

// Store processed message offsets to handle duplicates
const processedMessages = new Map<string, string>();

const normalizeEmail = (email: string): string => email.trim().toLowerCase();

async function processUserEvent(
    message: Buffer | null,
    key: string,
    offset: string,
): Promise<void> {
    if (!message) {
        return;
    }

    const messageId = `${key}-${offset}`;

    // Check for duplicate messages
    if (processedMessages.has(messageId)) {
        console.log(
            `[Consumer] Duplicate message detected for key: ${key} offset: ${offset}, skipping...`,
        );
        return;
    }

    try {
        const eventData: UserEvent = JSON.parse(message.toString());
        const newUsers = Array.isArray(eventData.data) ? eventData.data : [];

        console.log(
            `[Consumer] Processing batch event with ${newUsers.length} users at ${eventData.timestamp}`,
        );

        if (newUsers.length === 0) {
            processedMessages.set(messageId, messageId);
            return;
        }

        const cacheKey = "all_users";
        const cached = await RedisService.get(cacheKey);
        const parsedCache = cached ? JSON.parse(cached) : [];
        const allUsers: UserRow[] = Array.isArray(parsedCache)
            ? parsedCache
            : [];

        const incomingValidUsers = newUsers.filter((user): user is UserRow =>
            Boolean(user?.email),
        );

        const incomingEmailSet = new Set<string>();
        const dedupedIncoming: UserRow[] = [];

        for (let i = incomingValidUsers.length - 1; i >= 0; i--) {
            const user = incomingValidUsers[i];
            const email = normalizeEmail(user.email);
            if (incomingEmailSet.has(email)) {
                continue;
            }
            incomingEmailSet.add(email);
            dedupedIncoming.unshift(user);
        }

        const filteredExisting = allUsers.filter(
            (user) => !incomingEmailSet.has(normalizeEmail(user.email)),
        );
        const mergedUsers = [...dedupedIncoming, ...filteredExisting];

        await RedisService.set(cacheKey, JSON.stringify(mergedUsers), 3600);

        processedMessages.set(messageId, messageId);

        // Keep only last 1000 processed messages to avoid memory leaks
        if (processedMessages.size > 1000) {
            const firstKey = Array.from(processedMessages.keys())[0];
            processedMessages.delete(firstKey);
        }

        console.log(
            `[Consumer] Batch processing completed. Cached ${dedupedIncoming.length} users`,
        );
    } catch (error) {
        console.error(
            `[Consumer] Failed to process message for key ${key}:`,
            error,
        );
        throw error;
    }
}

async function startConsumer(): Promise<void> {
    try {
        console.log("[Consumer] Starting Kafka consumer...");
        console.log(`[Consumer] Connecting to topic: ${TOPIC}`);

        await KafkaService.subscribeToTopic(TOPIC, async (message) => {
            const key = message.key?.toString() || "unknown";
            const offset = message.offset;
            await processUserEvent(message.value, key, offset);
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
