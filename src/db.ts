import { Pool } from "pg";

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    database: "is-assignment",
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    max: 5,
});


export default pool;
