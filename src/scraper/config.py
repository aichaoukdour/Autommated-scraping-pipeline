import logging
from dataclasses import dataclass

@dataclass
class ScraperConfig:
    """Scraper and ETL configuration settings"""
    base_url: str = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    max_retries: int = 3
    wait_timeout: int = 5
    page_load_delay: int = 3
    section_load_delay: int = 2
    max_workers: int = 3
    headless: bool = True
    # Database Settings
    db_dsn: str = "dbname=hs user=postgres password=postgres host=localhost port=5433"

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger("ADIL_Pipeline")

logger = setup_logging()
