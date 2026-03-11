import { createClient, RedisClientType } from "redis";

let client: RedisClientType | null = null;
let connectingPromise: Promise<void> | null = null;

const getRedisUrl = (): string => {
    if (process.env.REDIS_URL) {
        return process.env.REDIS_URL;
    }

    const host = process.env.REDIS_HOST || "localhost";
    const port = process.env.REDIS_PORT || "6379";

    if (host.startsWith("redis://") || host.startsWith("rediss://")) {
        const hasPort = /:\d+$/.test(host);
        return hasPort ? host : `${host}:${port}`;
    }

    return `redis://${host}:${port}`;
};

const getClient = (): RedisClientType => {
    if (!client) {
        client = createClient({
            url: getRedisUrl(),
        });
    }

    return client;
};

const ensureConnected = async (): Promise<RedisClientType> => {
    const redisClient = getClient();

    if (redisClient.isOpen) {
        return redisClient;
    }

    if (!connectingPromise) {
        connectingPromise = redisClient
            .connect()
            .then((): void => undefined)
            .finally(() => {
                connectingPromise = null;
            });
    }

    await connectingPromise;
    return redisClient;
};


const connect = async (): Promise<void> => {
    try {
        await ensureConnected();
        console.log("[RedisService] Connected to Redis");
    } catch (error) {
        console.error("[RedisService] Failed to connect to Redis:", error);
    }
};

const get = async (key: string): Promise<string | null> => {
    try {
        const redisClient = await ensureConnected();
        const result = (await redisClient.get(key)) as string | null;
        return result;
    } catch (error) {
        console.error("[RedisService] GET error for key:", key, error);
        return null;
    }
};

const set = async (key: string, value: string, ttl?: number): Promise<void> => {
    try {
        const redisClient = await ensureConnected();
        if (ttl) {
            await redisClient.setEx(key, ttl, value);
        } else {
            await redisClient.set(key, value);
        }
    } catch (error) {
        console.error("[RedisService] SET error for key:", key, error);
    }
};

const del = async (key: string): Promise<void> => {
    try {
        const redisClient = await ensureConnected();
        await redisClient.del(key);
    } catch (error) {
        console.error("[RedisService] DEL error for key:", key, error);
    }
};

const isConnected = async (): Promise<boolean> => {
    try {
        const redisClient = getClient();
        if (!redisClient.isOpen) {
            return false;
        }

        await redisClient.ping();
        return true;
    } catch (error) {
        return false;
    }
};

const disconnect = async (): Promise<void> => {
    try {
        if (!client?.isOpen) {
            return;
        }

        await client.disconnect();
        console.log("[RedisService] Disconnected from Redis");
    } catch (error) {
        console.error("[RedisService] Error disconnecting:", error);
    }
};

export const RedisService = {
    connect,
    get,
    set,
    del,
    isConnected,
    disconnect,
};
