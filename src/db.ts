import { Pool } from "pg";

const pool = new Pool({
    database: process.env.PG_DB,
    host: process.env.PG_HOST,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    port: Number(process.env.PG_PORT),
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    max: 5,
});


export default pool;
