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
    });

    describe("uploadCsv", () => {
        it("should successfully parse and upload CSV data", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC\nJane Smith,jane@test.com,LA"
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest.fn().mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.query).toHaveBeenCalled();
            expect(mockKafkaService.publishEvent).toHaveBeenCalledWith(
                "user-events",
                expect.arrayContaining([
                    expect.objectContaining({
                        key: "john@test.com",
                    }),
                ])
            );
            expect(mockClient.release).toHaveBeenCalled();
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
                "CSV must contain at least name and email columns"
            );
        });

        it("should handle CSV with only headers", async () => {
            const csvBuffer = Buffer.from("name,email,city");

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(false);
        });

        it("should handle database connection errors gracefully", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC"
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
                "name,email,city\nJohn Doe,john@test.com,NYC"
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest.fn().mockRejectedValue(
                new Error("Kafka Error")
            );

            // Should not fail the upload just because Kafka failed
            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should handle optional city column", async () => {
            const csvBuffer = Buffer.from(
                "name,email\nJohn Doe,john@test.com\nJane Smith,jane@test.com"
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest.fn().mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
            expect(mockClient.query).toHaveBeenCalled();
        });

        it("should handle CSV with whitespace and line endings", async () => {
            const csvBuffer = Buffer.from(
                "name, email, city\r\nJohn Doe, john@test.com, NYC\r\nJane Smith, jane@test.com, LA"
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest.fn().mockResolvedValue(undefined);

            const result = await DataService.uploadCsv(csvBuffer);

            expect(result.success).toBe(true);
        });

        it("should use ON CONFLICT clause for duplicate emails", async () => {
            const csvBuffer = Buffer.from(
                "name,email,city\nJohn Doe,john@test.com,NYC"
            );

            const mockClient = {
                query: jest.fn().mockResolvedValue({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockKafkaService.publishEvent = jest.fn().mockResolvedValue(undefined);

            await DataService.uploadCsv(csvBuffer);

            const queryCall = mockClient.query.mock.calls[0];
            expect(queryCall[0]).toContain("ON CONFLICT (email)");
        });
    });

    describe("fetchData", () => {
        it("should fetch data from cache when available", async () => {
            const cachedData = {
                data: [{ id: 1, name: "John", email: "john@test.com", city: "NYC" }],
                metadata: {
                    page: 1,
                    limit: 10,
                    total: 1,
                    totalPages: 1,
                },
                cached: false,
            };

            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest.fn().mockResolvedValue(JSON.stringify(cachedData));

            const result = await DataService.fetchData(1, 10);

            expect(result.cached).toBe(true);
            expect(mockRedisService.get).toHaveBeenCalledWith("users:page:1:limit:10");
        });

        it("should fetch data from database when cache is empty", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "5" }] }) // Count query
                    .mockResolvedValueOnce({
                        rows: [
                            { id: 1, name: "John", email: "john@test.com", city: "NYC" },
                        ],
                    }), // Data query
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            const result = await DataService.fetchData(1, 10);

            expect(result.data).toHaveLength(1);
            expect(result.metadata.page).toBe(1);
            expect(result.metadata.limit).toBe(10);
            expect(result.cached).toBe(false);
            expect(mockClient.release).toHaveBeenCalled();
        });

        it("should calculate correct pagination metadata", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "25" }] })
                    .mockResolvedValueOnce({
                        rows: Array.from({ length: 10 }, (_, i) => ({
                            id: i + 11,
                            name: `User ${i + 11}`,
                            email: `user${i + 11}@test.com`,
                            city: "City",
                        })),
                    }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            const result = await DataService.fetchData(2, 10);

            expect(result.metadata.total).toBe(25);
            expect(result.metadata.totalPages).toBe(3);
            expect(result.metadata.page).toBe(2);
            expect(result.metadata.limit).toBe(10);
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

        it("should fall back to database when Redis connection fails", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "5" }] })
                    .mockResolvedValueOnce({
                        rows: [
                            { id: 1, name: "John", email: "john@test.com", city: "NYC" },
                        ],
                    }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockRejectedValue(new Error("Redis Error"));

            const result = await DataService.fetchData(1, 10);

            expect(result.data).toBeDefined();
            expect(result.cached).toBe(false);
        });

        it("should cache results with 5 minute TTL", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "1" }] })
                    .mockResolvedValueOnce({
                        rows: [
                            { id: 1, name: "John", email: "john@test.com", city: "NYC" },
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
                "users:page:1:limit:10",
                expect.any(String),
                300 // 5 minutes
            );
        });

        it("should handle cache storage failures gracefully", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "1" }] })
                    .mockResolvedValueOnce({
                        rows: [
                            { id: 1, name: "John", email: "john@test.com", city: "NYC" },
                        ],
                    }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(true);
            mockRedisService.get = jest.fn().mockResolvedValue(null);
            mockRedisService.set = jest.fn().mockRejectedValue(new Error("Cache Error"));

            const result = await DataService.fetchData(1, 10);

            expect(result.data).toBeDefined();
            expect(result.cached).toBe(false);
        });

        it("should apply correct offset for pagination", async () => {
            const mockClient = {
                query: jest
                    .fn()
                    .mockResolvedValueOnce({ rows: [{ total: "100" }] })
                    .mockResolvedValueOnce({ rows: [] }),
                release: jest.fn(),
            };

            (pool.connect as jest.Mock).mockResolvedValue(mockClient);
            mockRedisService.isConnected = jest.fn().mockResolvedValue(false);

            await DataService.fetchData(3, 10);

            const dataQuery = mockClient.query.mock.calls[1];
            expect(dataQuery[1]).toEqual([10, 20]); // LIMIT 10 OFFSET 20
        });
    });
});
