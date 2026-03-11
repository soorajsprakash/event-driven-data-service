import pool from "../db";

const initialisePgDb = async () => {
    try {
        await pool.connect();
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name text NOT NULL,
                email text NOT NULL UNIQUE,
                city text
            );
        `);
    } catch (err) {
        console.error("failed to ensure users table exists", err);
    }
};

export const PgService = {
    initialisePgDb,
};
