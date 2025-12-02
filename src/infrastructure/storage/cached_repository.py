"""
Cached repository - combines PostgreSQL and Redis
Uses decorator pattern for cache-first strategy
"""

import logging
from typing import Optional

from ...domain.entities import ScrapedData
from ...domain.repositories import FileRepository
from .postgresql_repository import PostgreSQLRepository
from .redis_cache_repository import RedisCacheRepository

logger = logging.getLogger(__name__)


class CachedRepository(FileRepository):
    """
    Repository that combines PostgreSQL (persistent) and Redis (cache)
    Implements cache-first strategy for optimal performance
    """
    
    def __init__(
        self,
        db_repository: PostgreSQLRepository,
        cache_repository: RedisCacheRepository,
        cache_ttl: int = 3600
    ):
        """
        Initialize cached repository
        
        Args:
            db_repository: PostgreSQL repository for persistent storage
            cache_repository: Redis repository for caching
            cache_ttl: Cache TTL in seconds (default: 1 hour)
        """
        self.db = db_repository
        self.cache = cache_repository
        self.cache_ttl = cache_ttl
        logger.info("Cached repository initialized")
    
    def save(self, data: ScrapedData, filepath: str = None) -> bool:
        """
        Save data with dual-write strategy:
        1. Save to PostgreSQL (persistent)
        2. Update Redis cache (immediate access)
        """
        # Save to database first
        db_success = self.db.save(data)
        
        if db_success:
            # Update cache
            cache_success = self.cache.set(data.tariff_code_searched, data, self.cache_ttl)
            if not cache_success:
                logger.warning(f"Failed to update cache for {data.tariff_code_searched}")
            return True
        
        return False
    
    def load(self, filepath: str) -> Optional[ScrapedData]:
        """
        Load data with cache-first strategy:
        1. Try Redis cache (fast)
        2. If miss, load from PostgreSQL
        3. Cache result for next time
        """
        # filepath is treated as tariff_code
        tariff_code = filepath
        
        # Try cache first
        cached_data = self.cache.get(tariff_code)
        if cached_data:
            logger.debug(f"Cache hit for {tariff_code}")
            return cached_data
        
        # Cache miss - load from database
        logger.debug(f"Cache miss for {tariff_code}, loading from database")
        db_data = self.db.load_latest(tariff_code)
        
        if db_data:
            # Cache for next time
            self.cache.set(tariff_code, db_data, self.cache_ttl)
        
        return db_data
    
    def load_latest(self, tariff_code: str) -> Optional[ScrapedData]:
        """Load latest version (same as load but explicit)"""
        return self.load(tariff_code)
    
    def load_history(self, tariff_code: str, limit: int = 10):
        """Load version history (always from database)"""
        return self.db.load_history(tariff_code, limit)
    
    def load_tariff_codes(self, filepath: str = None) -> list:
        """Load tariff codes from database"""
        return self.db.load_tariff_codes(filepath or "")
    
    def save_all(self, data_list: list, filepath: str = None) -> bool:
        """Save multiple records"""
        all_success = True
        for data in data_list:
            if not self.save(data):
                all_success = False
        return all_success
    
    def log_change(
        self,
        tariff_code: str,
        change_type: str,
        old_data: Optional[ScrapedData],
        new_data: ScrapedData,
        changes_summary: Optional[dict] = None
    ) -> bool:
        """Log change to database"""
        return self.db.log_change(tariff_code, change_type, old_data, new_data, changes_summary)
    
    def log_monitoring_activity(
        self,
        tariff_code: str,
        action: str,
        status: str,
        duration_ms: Optional[int] = None,
        message: Optional[str] = None
    ) -> bool:
        """Log monitoring activity"""
        return self.db.log_monitoring_activity(tariff_code, action, status, duration_ms, message)
    
    def invalidate_cache(self, tariff_code: str) -> bool:
        """Invalidate cache for a specific tariff code"""
        return self.cache.delete(tariff_code)
    
    def clear_cache(self) -> bool:
        """Clear all cache (use with caution!)"""
        return self.cache.clear_all()

