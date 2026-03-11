import { KafkaService } from "../../services/kafka.service";
import { RedisService } from "../../services/redis.service";

jest.mock("../../services/kafka.service");
jest.mock("../../services/redis.service");

type KafkaMessage = {
    key?: Buffer;
    value: Buffer | null;
    offset: string;
};

describe("Kafka Consumer", () => {
    let subscribedHandler: ((message: KafkaMessage) => Promise<void>) | null =
        null;

    const mockKafkaService = KafkaService as jest.Mocked<typeof KafkaService>;
    const mockRedisService = RedisService as jest.Mocked<typeof RedisService>;

    beforeAll(async () => {
        jest.spyOn(process, "on").mockImplementation((() => process) as any);

        mockKafkaService.subscribeToTopic = jest
            .fn()
            .mockImplementation(async (_topic, handler) => {
                subscribedHandler = handler as (
                    message: KafkaMessage,
                ) => Promise<void>;
            });

        mockKafkaService.disconnect = jest.fn().mockResolvedValue(undefined);
        mockRedisService.get = jest.fn().mockResolvedValue(null);
        mockRedisService.set = jest.fn().mockResolvedValue("OK");

        await import("../../kafka-consumer");

        expect(mockKafkaService.subscribeToTopic).toHaveBeenCalledWith(
            process.env.KAFKA_TOPIC,
            expect.any(Function),
        );
        expect(subscribedHandler).not.toBeNull();
    });

    beforeEach(() => {
        jest.clearAllMocks();
        mockRedisService.get = jest.fn().mockResolvedValue(null);
        mockRedisService.set = jest.fn().mockResolvedValue("OK");
    });

    afterAll(() => {
        jest.restoreAllMocks();
    });

    const makeMessage = (
        value: unknown,
        offset: string,
        key = "DATA_UPLOADED",
    ): KafkaMessage => ({
        key: Buffer.from(key),
        offset,
        value: Buffer.from(JSON.stringify(value)),
    });

    it("prepends incoming rows and removes old duplicates by email", async () => {
        const cachedUsers = [
            { name: "John Old", email: "john@test.com", city: "NYC" },
            { name: "Alice", email: "alice@test.com", city: "LA" },
        ];

        mockRedisService.get = jest
            .fn()
            .mockResolvedValueOnce(JSON.stringify(cachedUsers));

        await subscribedHandler!(
            makeMessage(
                {
                    timestamp: "2026-03-11T10:00:00Z",
                    data: [
                        {
                            name: "John New",
                            email: "JOHN@test.com",
                            city: "SF",
                        },
                        { name: "Bob", email: "bob@test.com", city: "TX" },
                    ],
                },
                "1",
            ),
        );

        expect(mockRedisService.set).toHaveBeenCalledTimes(1);
        const [cacheKey, cacheValue, ttl] = (mockRedisService.set as jest.Mock)
            .mock.calls[0];

        expect(cacheKey).toBe("all_users");
        expect(ttl).toBe(3600);

        const merged = JSON.parse(cacheValue as string);
        expect(merged).toEqual([
            { name: "John New", email: "JOHN@test.com", city: "SF" },
            { name: "Bob", email: "bob@test.com", city: "TX" },
            { name: "Alice", email: "alice@test.com", city: "LA" },
        ]);
    });

    it("creates cache from incoming data when cache does not exist", async () => {
        mockRedisService.get = jest.fn().mockResolvedValue(null);

        await subscribedHandler!(
            makeMessage(
                {
                    timestamp: "2026-03-11T10:00:00Z",
                    data: [
                        { name: "Jane", email: "jane@test.com", city: "LDN" },
                        { name: "Tom", email: "tom@test.com", city: "BER" },
                    ],
                },
                "2",
            ),
        );

        const [, cacheValue] = (mockRedisService.set as jest.Mock).mock
            .calls[0];
        const merged = JSON.parse(cacheValue as string);

        expect(merged).toEqual([
            { name: "Jane", email: "jane@test.com", city: "LDN" },
            { name: "Tom", email: "tom@test.com", city: "BER" },
        ]);
    });

    it("dedupes incoming chunk by email and keeps first occurrence", async () => {
        await subscribedHandler!(
            makeMessage(
                {
                    timestamp: "2026-03-11T10:00:00Z",
                    data: [
                        { name: "First", email: "dup@test.com", city: "A" },
                        { name: "Second", email: "DUP@test.com", city: "B" },
                    ],
                },
                "3",
            ),
        );

        const [, cacheValue] = (mockRedisService.set as jest.Mock).mock
            .calls[0];
        const merged = JSON.parse(cacheValue as string);

        expect(merged).toEqual([
            { name: "Second", email: "DUP@test.com", city: "B" },
        ]);
    });

    it("skips duplicate Kafka message based on key-offset", async () => {
        const message = makeMessage(
            {
                timestamp: "2026-03-11T10:00:00Z",
                data: [{ name: "Jane", email: "jane@test.com", city: "LDN" }],
            },
            "10",
        );

        await subscribedHandler!(message);
        await subscribedHandler!(message);

        expect(mockRedisService.set).toHaveBeenCalledTimes(1);
    });

    it("throws on invalid JSON payload", async () => {
        const invalidMessage: KafkaMessage = {
            key: Buffer.from("DATA_UPLOADED"),
            offset: "11",
            value: Buffer.from("invalid-json"),
        };

        await expect(subscribedHandler!(invalidMessage)).rejects.toThrow();
    });
});
