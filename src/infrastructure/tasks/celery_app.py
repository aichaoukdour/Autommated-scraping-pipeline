"""
Celery application and tasks
"""

import os
import logging
from celery import Celery
from celery.schedules import crontab

from ...domain.entities import TariffCode
from ...infrastructure.scraping.playwright_repository import PlaywrightScrapingRepository
from ...infrastructure.storage.postgresql_repository import PostgreSQLRepository
from ...infrastructure.storage.redis_cache_repository import RedisCacheRepository
from ...infrastructure.storage.cached_repository import CachedRepository
from ...infrastructure.search.elasticsearch_repository import ElasticsearchRepository
from ...domain.value_objects import ScrapingConfiguration

logger = logging.getLogger(__name__)

# Create Celery app
celery_app = Celery(
    'adil_scraper',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes
    task_soft_time_limit=240,  # 4 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50
)

# Periodic tasks schedule
celery_app.conf.beat_schedule = {
    'monitor-tariff-codes': {
        'task': 'src.infrastructure.tasks.celery_app.monitor_all_tariff_codes',
        'schedule': crontab(minute='*/60'),  # Every hour
    },
}


@celery_app.task(name='scrape_tariff_code', bind=True, max_retries=3)
def scrape_tariff_code_task(self, tariff_code: str):
    """
    Celery task to scrape a tariff code
    
    Args:
        tariff_code: Tariff code to scrape
    """
    logger.info(f"Starting scrape task for {tariff_code}")
    
    try:
        # Initialize repositories
        config = ScrapingConfiguration(
            headless=os.getenv('PLAYWRIGHT_HEADLESS', 'true').lower() == 'true',
            base_url="https://www.douane.gov.ma/adil/"
        )
        scraper = PlaywrightScrapingRepository(config)
        db_repo = PostgreSQLRepository()
        cache_repo = RedisCacheRepository()
        storage_repo = CachedRepository(db_repo, cache_repo)
        es_repo = ElasticsearchRepository()
        
        # Scrape
        scraped_data = scraper.scrape(TariffCode(tariff_code))
        
        if not scraped_data:
            raise Exception(f"Scraping returned no data for {tariff_code}")
        
        # Save to database
        success = storage_repo.save(scraped_data)
        
        if success:
            # Index in Elasticsearch
            es_repo.index_tariff_code(tariff_code, scraped_data.to_dict())
            logger.info(f"Successfully scraped and saved {tariff_code}")
            return {
                "status": "success",
                "tariff_code": tariff_code,
                "sections": len(scraped_data.sections)
            }
        else:
            logger.warning(f"Scrape completed but save returned False for {tariff_code}")
            return {
                "status": "skipped",
                "tariff_code": tariff_code,
                "message": "Data already exists (duplicate)"
            }
    
    except Exception as e:
        logger.error(f"Error scraping {tariff_code}: {e}", exc_info=True)
        # Retry with exponential backoff
        raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))


@celery_app.task(name='monitor_all_tariff_codes')
def monitor_all_tariff_codes():
    """
    Periodic task to monitor all tariff codes
    """
    logger.info("Starting periodic monitoring of all tariff codes")
    
    try:
        db_repo = PostgreSQLRepository()
        codes = db_repo.list_all_tariff_codes()
        
        logger.info(f"Found {len(codes)} tariff codes to monitor")
        
        # Queue scrape tasks for all codes
        for code in codes:
            scrape_tariff_code_task.delay(code)
        
        return {
            "status": "success",
            "codes_queued": len(codes)
        }
    except Exception as e:
        logger.error(f"Error in periodic monitoring: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }

