import { createClient, RedisClientType } from "redis";

const client: RedisClientType = createClient({
    url: process.env.REDIS_URL,
});


const connect = async (): Promise<void> => {
    try {
        await client.connect();
        console.log("[RedisService] Connected to Redis");
    } catch (error) {
        console.error("[RedisService] Failed to connect to Redis:", error);
    }
};

const get = async (key: string): Promise<string | null> => {
    try {
        const result = (await client.get(key)) as string | null;
        return result;
    } catch (error) {
        console.error("[RedisService] GET error for key:", key, error);
        return null;
    }
};

const set = async (key: string, value: string, ttl?: number): Promise<void> => {
    try {
        if (ttl) {
            await client.setEx(key, ttl, value);
        } else {
            await client.set(key, value);
        }
    } catch (error) {
        console.error("[RedisService] SET error for key:", key, error);
    }
};

const del = async (key: string): Promise<void> => {
    try {
        await client.del(key);
    } catch (error) {
        console.error("[RedisService] DEL error for key:", key, error);
    }
};

const isConnected = async (): Promise<boolean> => {
    try {
        await client.ping();
        return true;
    } catch (error) {
        return false;
    }
};

const disconnect = async (): Promise<void> => {
    try {
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

connect();
