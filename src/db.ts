import { Pool } from "pg";

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    database: "is-assignment",
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    max: 5,
});

// (async () => {
//     try {
//         await pool.query(`
//             CREATE TABLE IF NOT EXISTS users (
//                 id SERIAL PRIMARY KEY,
//                 name text NOT NULL,
//                 email text NOT NULL UNIQUE,
//                 city text
//             );
//         `);
//     } catch (err) {
//         console.error("failed to ensure users table exists", err);
//     }
// })();

export default pool;
