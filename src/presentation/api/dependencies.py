"""
Dependency injection for FastAPI
"""

import os
import logging
from functools import lru_cache

from ...infrastructure.storage.postgresql_repository import PostgreSQLRepository
from ...infrastructure.storage.cleaned_repository import CleanedDataRepository
from ...infrastructure.search.elasticsearch_repository import ElasticsearchRepository

logger = logging.getLogger(__name__)


@lru_cache()
def get_db_repo() -> PostgreSQLRepository:
    """Get PostgreSQL repository instance"""
    return PostgreSQLRepository()


@lru_cache()
def get_cleaned_repo() -> CleanedDataRepository:
    """Get cleaned data repository instance"""
    return CleanedDataRepository()


@lru_cache()
def get_elasticsearch_repo() -> ElasticsearchRepository:
    """Get Elasticsearch repository instance"""
    es_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
    return ElasticsearchRepository(es_url)

