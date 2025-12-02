"""
Script to index existing scraped data in Elasticsearch
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set environment
os.environ['DATABASE_URL'] = os.getenv('DATABASE_URL', 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper')
os.environ['ELASTICSEARCH_URL'] = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')

from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
from src.infrastructure.search.elasticsearch_repository import ElasticsearchRepository


def index_all_data():
    """Index all existing scraped data in Elasticsearch"""
    logger.info("Starting Elasticsearch indexing...")
    
    db_repo = PostgreSQLRepository()
    es_repo = ElasticsearchRepository()
    
    # Get all tariff codes
    codes = db_repo.list_all_tariff_codes()
    logger.info(f"Found {len(codes)} tariff codes to index")
    
    indexed = 0
    failed = 0
    
    for code in codes:
        try:
            data = db_repo.load_latest(code)
            if data:
                success = es_repo.index_tariff_code(code, data.to_dict())
                if success:
                    indexed += 1
                    logger.info(f"✓ Indexed {code}")
                else:
                    failed += 1
                    logger.warning(f"✗ Failed to index {code}")
            else:
                logger.warning(f"No data found for {code}")
                failed += 1
        except Exception as e:
            logger.error(f"Error indexing {code}: {e}", exc_info=True)
            failed += 1
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Indexing complete!")
    logger.info(f"  Indexed: {indexed}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    index_all_data()

