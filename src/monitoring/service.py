"""
Entry point for real-time monitoring service
Sets up dependencies and starts the service
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain.value_objects import ScrapingConfiguration, MonitoringConfiguration
from src.infrastructure.scraping import PlaywrightScrapingRepository
from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
from src.infrastructure.storage.redis_cache_repository import RedisCacheRepository
from src.infrastructure.storage.cached_repository import CachedRepository
from src.infrastructure.monitoring.service import RealTimeScrapingService


def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/monitoring.log') if os.path.exists('logs') else logging.NullHandler()
        ]
    )


def main():
    """Main entry point for monitoring service"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration from environment
        scraping_config = ScrapingConfiguration(
            headless=os.getenv('PLAYWRIGHT_HEADLESS', 'true').lower() == 'true',
            base_url="https://www.douane.gov.ma/adil/"
        )
        
        monitoring_config = MonitoringConfiguration(
            interval_minutes=int(os.getenv('MONITORING_INTERVAL_MINUTES', '60')),
            cache_ttl_seconds=int(os.getenv('CACHE_TTL_SECONDS', '3600')),
            enable_change_detection=os.getenv('ENABLE_CHANGE_DETECTION', 'true').lower() == 'true',
            save_only_on_change=False
        )
        
        logger.info("Initializing repositories...")
        
        # Initialize repositories
        db_repo = PostgreSQLRepository()
        cache_repo = RedisCacheRepository(
            default_ttl=monitoring_config.cache_ttl_seconds
        )
        storage_repo = CachedRepository(
            db_repository=db_repo,
            cache_repository=cache_repo,
            cache_ttl=monitoring_config.cache_ttl_seconds
        )
        
        scraping_repo = PlaywrightScrapingRepository(scraping_config)
        
        # Create monitoring service
        logger.info("Creating real-time scraping service...")
        service = RealTimeScrapingService(
            scraping_repository=scraping_repo,
            storage_repository=storage_repo,
            config=monitoring_config
        )
        
        # Start service
        logger.info("Starting service...")
        service.start()
        
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

