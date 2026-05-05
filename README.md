# A backend system with postgres, redis and kafka

This repository contains an Express+TypeScript backend service with Postgres, Redis, Kafka integration. All infrastructure runs locally via Docker Compose.

## Tech Stack
* Nodejs
* PostgreSql
* Redis
* Kafka
* Docker


## Prerequisites

- Docker & Docker Compose installed
- Node.js (>= 18) and npm

## Getting Started

1. **Copy environment file**

    ```bash
    cp .env.example .env
    ```

    Update with your values.
    Usually if one or more services are runing in the host system, the default port may not work, in that case give different port.

    > Eg: 5433 instead of 5432 as postgres port.

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

6. **Run the tests**
    ```bash
    npm run test
    npm run test:coverage # for coverage report
    ```
    This is a separate process and also reads `.env`.

7. **Stop docker**

    ```bash
    docker-compose down
    ```


## Environment Variables

Configuration is entirely driven by environment variables:

- `PG_USER` – PostgreSQL user
- `PG_PASSWORD` – PostgreSQL password
- `PG_DB` – PostgreSQL database name
- `PG_HOST` – PostgreSQL host
- `PG_PORT` – PostgreSQL port

- `REDIS_HOST` – Redis connection host url without port
- `REDIS_PORT` – Redis port

- `KAFKA_TOPIC` – Kafka topic for sending upload data event
- `KAFKA_PORT` – Kafka port
- `KAFKA_BROKERS_HOST` – host for the kafka broker (we are working with one broker in this sample)
- `PORT` – port for the Express server
- `JWT_SECRET` – secret key used to sign and verify JWT tokens (defaults to `dev-secret` locally)



## API Endpoints

- `POST /data` – upload CSV file (multer `file` field; must be `text/csv`, max 5 MB)
- `GET /data?page=1&limit=10` – fetch paginated entries (requires `Authorization: Bearer <token>`)

## Components
1. <u>Upload API</u>


* Accept a CSV file via a REST endpoint
* Validate and process the file contents
* Persist the data into a PostgreSQL database
* After saving, publish an event to a Kafka topic
* Return a structured success or error response


2. <u>Fetch API</u>

* Expose a REST endpoint to retrieve all records
* Serve from Redis cache where available
* Handle cache unavailability gracefully with a fallback strategy

3. <u>Kafka Consumer Service</u>
* Run as a standalone Node.js process separate from the API
* Listen to the Kafka topic published by the Upload API
* On each message, update the Redis cache
* Handle failures, retries, and duplicate messages
* Log meaningful output for each event processed

## Architecture

```mermaid
flowchart TD
    Client(["Client"])

    subgraph EXPRESS["Express API Server"]
        direction TB

        subgraph UPLOAD_PATH["POST /data  —  CSV Upload"]
            direction TB
            VM["validateUpload Middleware\nMIME: text/csv  ·  Max 5 MB"]
            UC["DataApi.uploadDataFile"]
            UP_SVC["DataService\nParse CSV  ·  Deduplicate rows\nBatch upsert into PostgreSQL\nPublish Kafka events in 500-record chunks"]
        end

        subgraph FETCH_PATH["GET /data?page=1&limit=10  —  Paginated Fetch"]
            direction TB
            AM["authenticateToken Middleware\nJWT Bearer Token"]
            FC["DataApi.fetchDataFile"]
            FE_SVC["DataService\nCache-first lookup\nReturns: data · page · limit · total · totalPages\ncache field indicates source"]
        end
    end

    PG[("PostgreSQL\nusers  —  id · name · email · city\nUpsert on email conflict")]
    REDIS[("Redis\nfile:{sha256}  TTL 5 days\nall_users  TTL 1 hour")]
    KAFKA["Kafka Broker\nTopic: KAFKA_TOPIC\nKey: DATA_UPLOADED\n500-record batches per message"]

    subgraph CONSUMER["Kafka Consumer  (npm run consumer  —  standalone process)"]
        direction TB
        KSub["Subscribe to KAFKA_TOPIC"]
        KDedup["Deduplicate incoming users\nNormalise emails  ·  Merge with cached batch"]
        KWrite["Update Redis  all_users\nTTL 1 hour"]
    end

    Client -->|"multipart/form-data  field: file"| VM
    VM -->|"valid type & size"| UC
    UC --> UP_SVC
    UP_SVC -->|"batch upsert"| PG
    UP_SVC -->|"cache SHA-256 file hash"| REDIS
    UP_SVC -->|"publish DATA_UPLOADED events"| KAFKA

    Client -->|"Authorization: Bearer &lt;token&gt;"| AM
    AM -->|"valid JWT"| FC
    FC --> FE_SVC
    FE_SVC -->|"cache-first lookup"| REDIS
    REDIS -. "cache miss  →  fallback" .-> PG

    KAFKA -->|"consume"| KSub
    KSub --> KDedup
    KDedup --> KWrite
    KWrite -->|"write"| REDIS
```

## Notes

- Kafka events are published on upload and consumed by a standalone service
- Redis is used for caching and updated by the consumer
- All configuration defaults are safe for local development
- The fetch endpoint is protected with JWT Bearer token authentication
- The upload endpoint validates file type and size with Joi before processing

