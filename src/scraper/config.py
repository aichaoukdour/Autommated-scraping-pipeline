import os
import logging
import time
from contextlib import contextmanager
from psycopg2 import pool
from dotenv import load_dotenv
from dataclasses import dataclass, field

# Load environment variables from .env if it exists
load_dotenv()

@dataclass
class ScraperConfig:
    """Scraper and ETL configuration settings"""
    base_url: str = os.getenv("SCRAPER_BASE_URL", "https://www.douane.gov.ma/adil/c_bas_test_1.asp")
    max_retries: int = int(os.getenv("SCRAPER_MAX_RETRIES", "3"))
    wait_timeout: int = int(os.getenv("SCRAPER_WAIT_TIMEOUT", "5"))
    page_load_delay: int = int(os.getenv("SCRAPER_PAGE_LOAD_DELAY", "3"))
    section_load_delay: float = float(os.getenv("SCRAPER_SECTION_LOAD_DELAY", "1.5"))
    max_workers: int = int(os.getenv("SCRAPER_MAX_WORKERS", "3"))
    headless: bool = os.getenv("SCRAPER_HEADLESS", "true").lower() == "true"
    
    # Notification Settings
    webhook_url: str = os.getenv("NOTIFY_WEBHOOK_URL", "")

    # Database Settings
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5433") # Defaulting to your current port
    db_name: str = os.getenv("DB_NAME", "hs")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    def send_notification(self, message: str) -> bool:
        """Send a notification via webhook (Slack/Discord compatible)"""
        if not self.webhook_url:
            return False
        try:
            import requests
            response = requests.post(self.webhook_url, json={"text": message})
            return response.status_code < 300
        except Exception:
            return False

    @property
    def db_dsn(self) -> str:
        return f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}"

class ConnectionManager:
    """Manages a pool of database connections with automatic retries."""
    _pool = None

    @classmethod
    def initialize_pool(cls, config: ScraperConfig):
        if cls._pool is None:
            logger.info(f"Initializing connection pool (min=2, max={config.max_workers + 2})")
            cls._pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=config.max_workers + 2,
                dsn=config.db_dsn
            )

    @classmethod
    @contextmanager
    def get_connection(cls, timeout=30):
        """Context manager to get a connection from the pool with retry logic."""
        start_time = time.time()
        conn = None
        while time.time() - start_time < timeout:
            try:
                conn = cls._pool.getconn()
                yield conn
                cls._pool.putconn(conn)
                return
            except Exception as e:
                if conn:
                    cls._pool.putconn(conn, close=True)
                logger.warning(f"Database connection error: {e}. Retrying...")
                time.sleep(2)
        
        raise Exception("Could not acquire a database connection after multiple retries.")

    @classmethod
    def close_all(cls):
        if cls._pool:
            logger.info("Closing all database connections in the pool.")
            cls._pool.closeall()
            cls._pool = None

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger("ADIL_Pipeline")

logger = setup_logging()
