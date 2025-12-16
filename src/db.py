import asyncpg
import os
import logging

# Database Configuration
# Using default postgres/postgres for local dev if env vars not set
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres1234")

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASS,
                    database=DB_NAME
                )
                logging.info("Connected to database.")
                await self.init_db()
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logging.info("Database connection closed.")

    async def init_db(self):
        """Create table if not exists"""
        query = """
        CREATE TABLE IF NOT EXISTS products (
            hs_code VARCHAR(20) PRIMARY KEY,
            description TEXT,
            di_rate VARCHAR(10),
            tpi_rate VARCHAR(10),
            tva_rate VARCHAR(10),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query)

    async def upsert_product(self, data):
        """Insert or Update product data"""
        query = """
        INSERT INTO products (hs_code, description, di_rate, tpi_rate, tva_rate, updated_at)
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
        ON CONFLICT (hs_code) 
        DO UPDATE SET 
            description = EXCLUDED.description,
            di_rate = EXCLUDED.di_rate,
            tpi_rate = EXCLUDED.tpi_rate,
            tva_rate = EXCLUDED.tva_rate,
            updated_at = CURRENT_TIMESTAMP;
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    query,
                    data['hs_code_input'],
                    data['description'],
                    data['di_rate'],
                    data['tpi_rate'],
                    data['tva_rate']
                )
            logging.info(f"Saved to DB: {data['hs_code_input']}")
        except Exception as e:
            logging.error(f"Failed to save {data['hs_code_input']} to DB: {e}")
