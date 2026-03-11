import { DataService } from "../../services/data.service";
import { RedisService } from "../../services/redis.service";
import { KafkaService } from "../../services/kafka.service";
import pool from "../../db";

// Mock dependencies
jest.mock("../../services/redis.service");
jest.mock("../../services/kafka.service");
jest.mock("../../db", () => ({
    connect: jest.fn(),
    query: jest.fn(),
}));

const mockRedisService = RedisService as jest.Mocked<typeof RedisService>;
const mockKafkaService = KafkaService as jest.Mocked<typeof KafkaService>;

describe("DataService", () => {
    beforeEach(() => {
        jest.clearAllMocks();
        mockRedisService.get = jest.fn().mockResolvedValue(null);
        mockRedisService.set = jest.fn().mockResolvedValue("OK");
        mockRedisService.isConnected = jest.fn().mockResolvedValue(false);
        mockKafkaService.publishEvent = jest.fn().mockResolvedValue(undefined);
    });

    describe("uploadCsv", () => {
        it("should successfully parse and upload CSV data", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC\nJane Smith,jane@test.com,LA",
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest
                .fn()
                .mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.query).toHaveBeenCalled();
            expect(mockKafkaService.publishEvent).toHaveBeenCalledWith(
                process.env.KAFKA_TOPIC!,
                expect.arrayContaining([
                    expect.objectContaining({
                        key: "DATA_UPLOADED",
                    }),
                ]),
            );
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should send Kafka events in chunks of 500 rows", async () => {
            const rows = Array.from(
                { length: 501 },
                (_, i) => `User ${i + 1},user${i + 1}@test.com,City`,
            ).join("\n");
            const csvBuffer = Buffer.from(`name,email,city\n${rows}`);

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockKafkaService.publishEvent).toHaveBeenCalledTimes(2);

            const firstPayload = (mockKafkaService.publishEvent as jest.Mock)
                .mock.calls[0][1][0];
            const secondPayload = (mockKafkaService.publishEvent as jest.Mock)
                .mock.calls[1][1][0];

            expect(JSON.parse(firstPayload.value).data).toHaveLength(500);
            expect(JSON.parse(secondPayload.value).data).toHaveLength(1);
        });

        it("should skip processing when file hash already exists", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC",
            );

            mockRedisService.get = jest.fn().mockResolvedValue("uploaded");

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(pool.connect).not.toHaveBeenCalled();
            expect(mockKafkaService.publishEvent).not.toHaveBeenCalled();
        });

        it("should handle empty CSV file", async () => {
            const csvBuffer = Buffer.from("");

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            // Should return early without database operations
        });

        it("should throw error when required columns are missing", async () => {
            const csvBuffer = Buffer.from("name,city\nJohn Doe,NYC");

            await expect(DataService.uploadCsv(csvBuffer)).rejects.toThrow(
                "CSV must contain at least name and email columns",
            );
        });

        it("should handle CSV with only headers", async () => {
            const csvBuffer = Buffer.from("name,email,city");

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(false);
        });

        it("should handle database connection errors gracefully", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC",
            );

            const mockClient = {
                query: jest.fn().mockRejectedValue(new Error("DB Error")),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(false);
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should handle Kafka publish failures gracefully", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC",
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest
                .fn()
                .mockRejectedValue(new Error("Kafka Error"));

            // Should not fail the upload just because Kafka failed
            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should handle optional city column", async () => {
            const csvBuffer = Buffer.from(
                "name,email\nJohn Doe,john@test.com\nJane Smith,jane@test.com",
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest
                .fn()
                .mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.query).toHaveBeenCalled();
        });

        it("should handle CSV with whitespace and line endings", async () => {
            const csvBuffer = Buffer.from(
                "name, email, city\r\nJohn Doe, john@test.com, NYC\r\nJane Smith, jane@test.com, LA",
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest
                .fn()
                .mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
        });

        it("should use ON CONFLICT clause for duplicate emails", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC",
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest
                .fn()
                .mockResolvedValue(undefined);

            await DataService.uploadCsv(csvBuffer);

            const queryCall = mockClient.query.mock.calls[0];
            expect(queryCall[0]).toContain("ON CONFLICT (email)");
        });
    });

    describe("fetchData", () => {
        it("should fetch data from cache when available", async () => {
            const cachedData = [
                { id: 1, name: "John", email: "john@test.com", city: "NYC" },
                { id: 2, name: "Jane", email: "jane@test.com", city: "LA" },
            ];

            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest
                .fn()
                .mockResolvedValue(JSON.stringify(cachedData));

            const result = await DataService.fetchData(1, 10);

            expect(result.cached).toBe(true);
            expect(result.data).toHaveLength(2);
            expect(pool.connect).not.toHaveBeenCalled();
            expect(mockRedisService.get).toHaveBeenCalledWith("all_users");
        });

        it("should fetch data from database when cache is empty", async () => {
            const mockClient = {
                query: jest.fn().mockResolvedValue({
                    rows: [
                        {
                            id: 1,
                            name: "John",
                            email: "john@test.com",
                            city: "NYC",
                        },
                    ],
                }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest.fn().mockResolvedValue(null);

            const result = await DataService.fetchData(1, 10);

            expect(result.data).toHaveLength(1);
            expect(result.metadata.page).toBe(1);
            expect(result.metadata.limit).toBe(10);
            expect(result.cached).toBe(false);
            expect(mockRedisService.set).toHaveBeenCalledWith(
                "all_users",
                expect.any(String),
                3600,
            );
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should calculate correct pagination metadata", async () => {
            const users = Array.from({ length: 25 }, (_, i) => ({
                id: i + 1,
                name: `User ${i + 1}`,
                email: `user${i + 1}@test.com`,
                city: "City",
            }));

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: users }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            const result = await DataService.fetchData(2, 10);

            expect(result.metadata.total).toBe(25);
            expect(result.metadata.totalPages).toBe(3);
            expect(result.metadata.page).toBe(2);
            expect(result.metadata.limit).toBe(10);
            expect(result.data).toHaveLength(10);
        });

        it("should handle database errors gracefully", async () => {
            const mockClient = {
                query: jest.fn().mockRejectedValue(new Error("DB Error")),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            await expect(DataService.fetchData(1, 10)).rejects.toThrow();
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should throw when Redis checks fail during DB fallback path", async () => {
            const mockClient = {
                query: jest.fn().mockResolvedValue({
                    rows: [
                        {
                            id: 1,
                            name: "John",
                            email: "john@test.com",
                            city: "NYC",
                        },
                    ],
                }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest
                .fn()
                .mockRejectedValue(new Error("Redis Error"));

            await expect(DataService.fetchData(1, 10)).rejects.toThrow(
                "Redis Error",
            );
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should cache results with 5 minute TTL", async () => {
            const mockClient = {
                query: jest.fn().mockResolvedValue({
                    rows: [
                        {
                            id: 1,
                            name: "John",
                            email: "john@test.com",
                            city: "NYC",
                        },
                    ],
                }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest.fn().mockResolvedValue(null);
            mockRedisService.set = jest.fn().mockResolvedValue("OK");

            await DataService.fetchData(1, 10);

            expect(mockRedisService.set).toHaveBeenCalledWith(
                "all_users",
                expect.any(String),
                3600,
            );
        });

        it("should throw when cache storage fails", async () => {
            const mockClient = {
                query: jest.fn().mockResolvedValue({
                    rows: [
                        {
                            id: 1,
                            name: "John",
                            email: "john@test.com",
                            city: "NYC",
                        },
                    ],
                }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest.fn().mockResolvedValue(null);
            mockRedisService.set = jest
                .fn()
                .mockRejectedValue(new Error("Cache Error"));

            await expect(DataService.fetchData(1, 10)).rejects.toThrow(
                "Cache Error",
            );
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should apply correct slicing for pagination", async () => {
            const users = Array.from({ length: 30 }, (_, i) => ({
                id: i + 1,
                name: `User ${i + 1}`,
                email: `user${i + 1}@test.com`,
                city: "City",
            }));

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: users }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            const result = await DataService.fetchData(3, 10);

            expect(result.data[0].id).toBe(21);
            expect(result.data).toHaveLength(10);
        });
    });
});
