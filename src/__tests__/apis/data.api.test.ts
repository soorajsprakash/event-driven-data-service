import { DataApi } from "../../apis/data.api";
import { DataService } from "../../services/data.service";
import * as express from "express";

// Mock the DataService
jest.mock("../../services/data.service");

const mockDataService = DataService as jest.Mocked<typeof DataService>;

describe("DataApi", () => {
    let mockRequest: Partial<express.Request>;
    let mockResponse: Partial<express.Response>;
    let jsonMock: jest.Mock;
    let statusMock: jest.Mock;

    beforeEach(() => {
        jest.clearAllMocks();

        // Setup response mock
        jsonMock = jest.fn().mockReturnValue({});
        statusMock = jest.fn().mockReturnValue({ json: jsonMock });

        mockResponse = {
            json: jsonMock,
            status: statusMock,
        };

        mockRequest = {
            query: {},
            file: undefined,
        };
    });

    describe("uploadDataFile", () => {
        it("should successfully upload CSV file and return success response", async () => {
            const mockFile = {
                buffer: Buffer.from("name,email,city\nJohn,john@test.com,NYC"),
                originalname: "test.csv",
                size: 43,
            };

            mockRequest.file = mockFile as any;
            mockDataService.uploadCsv.mockResolvedValue({ success: true });

            await DataApi.uploadDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(mockDataService.uploadCsv).toHaveBeenCalledWith(mockFile.buffer);
            expect(jsonMock).toHaveBeenCalledWith({
                message: "Successfully uploaded the data file",
                data: { success: true },
            });
        });

        it("should return 400 error when no file is provided", async () => {
            mockRequest.file = undefined;

            await DataApi.uploadDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(400);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "No file uploaded under field 'file'",
            });
            expect(mockDataService.uploadCsv).not.toHaveBeenCalled();
        });

        it("should return 500 error when DataService throws an exception", async () => {
            const mockFile = {
                buffer: Buffer.from("name,email\nJohn"),
                originalname: "test.csv",
                size: 18,
            };

            mockRequest.file = mockFile as any;
            mockDataService.uploadCsv.mockRejectedValue(new Error("CSV parsing failed"));

            await DataApi.uploadDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(500);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "CSV parsing failed",
            });
        });

        it("should handle generic error messages", async () => {
            const mockFile = {
                buffer: Buffer.from("name,email\nJohn"),
                originalname: "test.csv",
                size: 18,
            };

            mockRequest.file = mockFile as any;
            mockDataService.uploadCsv.mockRejectedValue({});

            await DataApi.uploadDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(500);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "internal error",
            });
        });
    });

    describe("fetchDataFile", () => {
        it("should successfully fetch data with default pagination", async () => {
            const mockData = {
                data: [
                    { id: 1, name: "John", email: "john@test.com", city: "NYC" },
                    { id: 2, name: "Jane", email: "jane@test.com", city: "LA" },
                ],
                metadata: {
                    page: 1,
                    limit: 10,
                    total: 2,
                    totalPages: 1,
                },
                cached: false,
            };

            mockRequest.query = {};
            mockDataService.fetchData.mockResolvedValue(mockData);

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(mockDataService.fetchData).toHaveBeenCalledWith(1, 10);
            expect(jsonMock).toHaveBeenCalledWith({
                message: "Data retrieved successfully",
                data: mockData,
            });
        });

        it("should fetch data with custom pagination parameters", async () => {
            const mockData = {
                data: [{ id: 1, name: "John", email: "john@test.com", city: "NYC" }],
                metadata: {
                    page: 2,
                    limit: 5,
                    total: 10,
                    totalPages: 2,
                },
                cached: false,
            };

            mockRequest.query = { page: "2", limit: "5" };
            mockDataService.fetchData.mockResolvedValue(mockData);

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(mockDataService.fetchData).toHaveBeenCalledWith(2, 5);
            expect(jsonMock).toHaveBeenCalledWith({
                message: "Data retrieved successfully",
                data: mockData,
            });
        });

        it("should return 400 error when page is less than 1", async () => {
            mockRequest.query = { page: "-1", limit: "10" };

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(400);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "Invalid pagination parameters. Page must be >= 1, limit must be between 1 and 100",
            });
            expect(mockDataService.fetchData).not.toHaveBeenCalled();
        });

        it("should return 400 error when limit is less than 1", async () => {
            // Note: The code uses "||" which treats 0 as falsy, so we need negative numbers
            mockRequest.query = { page: "1", limit: "-1" };

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(400);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "Invalid pagination parameters. Page must be >= 1, limit must be between 1 and 100",
            });
            expect(mockDataService.fetchData).not.toHaveBeenCalled();
        });

        it("should return 400 error when limit exceeds 100", async () => {
            mockRequest.query = { page: "1", limit: "101" };

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(400);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "Invalid pagination parameters. Page must be >= 1, limit must be between 1 and 100",
            });
            expect(mockDataService.fetchData).not.toHaveBeenCalled();
        });

        it("should return 500 error when DataService throws an exception", async () => {
            mockRequest.query = { page: "1", limit: "10" };
            mockDataService.fetchData.mockRejectedValue(new Error("Database connection failed"));

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(statusMock).toHaveBeenCalledWith(500);
            expect(jsonMock).toHaveBeenCalledWith({
                error: "Database connection failed",
            });
        });

        it("should handle non-numeric query parameters gracefully", async () => {
            const mockData = {
                data: [] as any[],
                metadata: {
                    page: 1,
                    limit: 10,
                    total: 0,
                    totalPages: 0,
                },
                cached: false,
            };

            mockRequest.query = { page: "abc", limit: "def" };
            mockDataService.fetchData.mockResolvedValue(mockData);

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            // parseInt("abc") returns NaN, which should default to 1
            expect(mockDataService.fetchData).toHaveBeenCalledWith(1, 10);
        });

        it("should return cached data when available", async () => {
            const mockData = {
                data: [{ id: 1, name: "John", email: "john@test.com", city: "NYC" }],
                metadata: {
                    page: 1,
                    limit: 10,
                    total: 1,
                    totalPages: 1,
                },
                cached: true,
            };

            mockRequest.query = { page: "1", limit: "10" };
            mockDataService.fetchData.mockResolvedValue(mockData);

            await DataApi.fetchDataFile(mockRequest as express.Request, mockResponse as express.Response);

            expect(jsonMock).toHaveBeenCalledWith({
                message: "Data retrieved successfully",
                data: mockData,
            });
            expect(mockData.cached).toBe(true);
        });
    });
});
