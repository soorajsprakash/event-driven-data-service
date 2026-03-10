import pool from "../db";
import {
    UploadDataResponseModel,
    FetchDataResponseModel,
    UserData,
} from "src/models/data.response";
import { RedisService } from "./redis.service";

const uploadCsv = async (
    csvBuffer: Buffer,
): Promise<UploadDataResponseModel> => {
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

    return { success: true };
};

const fetchData = async (
    page: number = 1,
    limit: number = 10,
): Promise<FetchDataResponseModel> => {
    const offset = (page - 1) * limit;
    const cacheKey = `users:page:${page}:limit:${limit}`;

    // Try to get from cache first
    try {
        const isRedisConnected = await RedisService.isConnected();
        console.log("isRedisConnected: ", isRedisConnected);
        if (isRedisConnected) {
            const cachedData = await RedisService.get(cacheKey);
            console.log("cachedData: ", cachedData);
            if (cachedData) {
                const parsedData = JSON.parse(cachedData);
                return { ...parsedData, cached: true };
            }
        }
    } catch (error) {
        console.warn(
            "Cache retrieval failed, falling back to database:",
            error,
        );
    }

    // Fallback to database
    const client = await pool.connect();
    try {
        // Get total count
        const countResult = await client.query(
            "SELECT COUNT(*) as total FROM users",
        );
        const total = parseInt(countResult.rows[0].total);

        // Get paginated data
        const query = `
            SELECT id, name, email, city
            FROM users
            ORDER BY id
            LIMIT $1 OFFSET $2
        `;
        const result = await client.query(query, [limit, offset]);

        const data: UserData[] = result.rows;
        const totalPages = Math.ceil(total / limit);

        const response: FetchDataResponseModel = {
            data,
            metadata: {
                page,
                limit,
                total,
                totalPages,
            },
            cached: false,
        };

        // Try to cache the result
        try {
            const isRedisConnected = await RedisService.isConnected();
            console.log("isRedisConnected", isRedisConnected)
            if (isRedisConnected) {
                const res = await RedisService.set(
                    cacheKey,
                    JSON.stringify(response),
                    300,
                ); // Cache for 5 minutes
                console.log("Cache set result: ", res);
            }
        } catch (error) {
            console.warn("Cache storage failed:", error);
        }

        return response;
    } finally {
        client.release();
    }
};

export const DataService = {
    uploadCsv,
    fetchData,
};
