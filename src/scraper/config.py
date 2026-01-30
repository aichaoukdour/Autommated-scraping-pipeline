import os
import logging
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
    
    # Database Settings
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5433") # Defaulting to your current port
    db_name: str = os.getenv("DB_NAME", "hs")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    @property
    def db_dsn(self) -> str:
        return f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}"

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger("ADIL_Pipeline")

logger = setup_logging()
