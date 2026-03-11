module.exports = {
    preset: "ts-jest",
    testEnvironment: "node",
    roots: ["<rootDir>/src"],
    testMatch: ["**/__tests__/**/*.test.ts", "**/?(*.)+(spec|test).ts"],
    moduleNameMapper: {
        "^src/(.*)$": "<rootDir>/src/$1",
        "^apis/(.*)$": "<rootDir>/src/apis/$1",
        "^models/(.*)$": "<rootDir>/src/models/$1",
        "^services/(.*)$": "<rootDir>/src/services/$1",
        "^routes/(.*)$": "<rootDir>/src/routes/$1",
    },
    collectCoverageFrom: [
        "src/**/*.ts",
        "!src/**/*.d.ts",
        "!src/app.ts",
        "!src/kafka-consumer.ts",
        "!src/db.ts",
    ],
    coveragePathIgnorePatterns: ["/node_modules/"],
    moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
};
