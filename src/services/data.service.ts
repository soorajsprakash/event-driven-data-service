import pool from "../db";
import {
    UploadDataResponseModel,
    FetchDataResponseModel,
    UserData,
} from "src/models/data.response";
import { RedisService } from "./redis.service";
import { KafkaService } from "./kafka.service";
import crypto from "crypto";


const kafkaTopic = process.env.KAFKA_TOPIC;

const uploadCsv = async (csvBuffer: Buffer): Promise<UploadDataResponseModel> => {
    const fileHash = crypto
        .createHash("sha256")
        .update(csvBuffer)
        .digest("hex");
    const isFileProcessed = await RedisService.get(`file:${fileHash}`);
    if (isFileProcessed) {
        console.log(
            "Skipping file processing as it was already processed before (hash match)",
        );
        return { success: true };
    }

    const text = csvBuffer.toString("utf8");
    const lines = text
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter((l) => l.length > 0);
    if (lines.length === 0) {
        return { success: true };
    }

    const header = lines
        .shift()!
        .split(",")
        .map((h) => h.trim().toLowerCase());
    const idx = {
        name: header.indexOf("name"),
        email: header.indexOf("email"),
        city: header.indexOf("city"),
    };

    if (idx.name === -1 || idx.email === -1) {
        throw new Error("CSV must contain at least name and email columns");
    }

    const validRows: Array<{
        name: string;
        email: string;
        city: string;
    }> = [];

    for (const line of lines) {
        if (line === "") continue;
        const cells = line.split(",").map((c) => c.trim());

        const name = cells[idx.name];
        const email = cells[idx.email];
        const city = idx.city !== -1 ? cells[idx.city] : "";

        validRows.push({ name, email, city });
    }

    if (validRows.length === 0) {
        return {
            success: false,
        };
    }

    const client = await pool.connect();
    try {
        const values = validRows
            .map((u, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
            .join(",");

        const flatValues = validRows.flatMap((u) => [u.name, u.email, u.city]);

        const query = `
            INSERT INTO users (name, email, city)
            VALUES ${values}
            ON CONFLICT (email) DO UPDATE SET city = EXCLUDED.city, name = EXCLUDED.name
        `;
        await client.query(query, flatValues);
    } catch (err) {
        console.log("ERROR: ", err);
        client.release();
        return { success: false };
    } finally {
        client.release();
    }

    // Publish a single kafka event with the last 500 rows, which would ideally should be in the cache.
    for (let i = 0; i < validRows.length; i += 500) {
        const chunk = validRows.slice(i, i + 500);

        const kafkaMessages = [
            {
                key: "DATA_UPLOADED",
                value: JSON.stringify({
                    data: chunk,
                    timestamp: new Date().toISOString(),
                }),
            },
        ];
        try {
            await KafkaService.publishEvent(kafkaTopic, kafkaMessages);
            console.log(
                `Published an event with ${chunk.length} user events to Kafka`,
            );
        } catch (error) {
            console.warn("Kafka publish failed for chunk:", error);
        }
    }

    // save file hash in redis with ttl of 5 days to prevent re-processing the same file
    await RedisService.set(`file:${fileHash}`, "uploaded", 5 * 24 * 60 * 60);
    return { success: true };
};;

const fetchData = async (
    page: number = 1,
    pageSize: number = 10,
): Promise<FetchDataResponseModel> => {
    const cacheKey = "all_users";

    let users: UserData[] | null = null;
    let servedFromCache = false;

    try {
        const isRedisConnected = await RedisService.isConnected();
        if (isRedisConnected) {
            const cachedData = await RedisService.get(cacheKey);
            if (cachedData) {
                const parsed = JSON.parse(cachedData) as UserData[];
                users = Array.isArray(parsed) ? parsed : [];
                servedFromCache = users.length > 0 ? true : false;
            }
        }
    } catch (error) {
        console.warn(
            "Cache retrieval failed, falling back to database:",
            error,
        );
    }

    if (!users || users.length === 0) {
        const client = await pool.connect();
        try {
            const query = `
                SELECT id, name, email, city
                FROM users
                ORDER BY id
            `;
            const result = await client.query(query);
            users = result.rows;

            // set cache
            const isRedisConnected = await RedisService.isConnected();
            if (isRedisConnected && users.length > 0) {
                console.log(
                    "Setting cache for all users with",
                    users.length,
                    "entries",
                );
                await RedisService.set(
                    cacheKey,
                    JSON.stringify(users),
                    60 * 60,
                );
            }
        } finally {
            client.release();
        }
    }

    const total = users.length;
    const totalPages = Math.ceil(total / pageSize);
    const start = Math.max(0, (page - 1) * pageSize);
    const end = start + pageSize;

    return {
        data: users.slice(start, end),
        metadata: {
            page,
            limit: pageSize,
            total,
            totalPages,
        },
        cached: servedFromCache,
    };
};

export const DataService = {
    uploadCsv,
    fetchData,
};
