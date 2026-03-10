# IS Assignment Backend

This repository contains an Express+TypeScript backend service with Postgres, Redis, Kafka integration. All infrastructure runs locally via Docker Compose.

## Prerequisites

- Docker & Docker Compose installed
- Node.js (>= 18) and npm

## Getting Started

1. **Copy environment file**

    ```bash
    cp .env.example .env
    ```

    Edit values if needed (e.g. ports, passwords).

2. **Start infrastructure**

    ```bash
    docker-compose up -d
    ```

    This will bring up PostgreSQL, Redis, Zookeeper, and Kafka.

3. **Install dependencies**

    ```bash
    npm install
    ```

4. **Run the API server**

    ```bash
    npm run start
    ```

    The Node process will load variables from `.env` (via `dotenv`).

5. **Run the Kafka consumer**
    ```bash
    npm run consumer
    ```
    This is a separate process and also reads `.env`.

## Environment Variables

Configuration is entirely driven by environment variables:

- `DATABASE_URL` – PostgreSQL connection string
- `REDIS_URL` – Redis connection string
- `KAFKA_BROKERS` – comma-separated list of Kafka brokers
- `PORT` – port for the Express server

Other variables like `POSTGRES_USER`, etc. are used by Docker Compose.

## API Endpoints

- `POST /data` – upload CSV file (multer `file` field)
- `GET /data?page=1&limit=10` – fetch paginated entries

## Notes

- Kafka events are published on upload and consumed by a standalone service
- Redis is used for caching and updated by the consumer
- All configuration defaults are safe for local development
