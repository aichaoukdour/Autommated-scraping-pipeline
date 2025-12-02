"""
Redis cache repository implementation
"""

import json
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import redis
from redis.exceptions import RedisError

from ...domain.entities import ScrapedData

logger = logging.getLogger(__name__)


class RedisCacheRepository:
    """Redis-based cache repository"""
    
    def __init__(self, redis_url: Optional[str] = None, default_ttl: int = 3600):
        """
        Initialize Redis cache repository
        
        Args:
            redis_url: Redis connection URL
                      Defaults to REDIS_URL environment variable
            default_ttl: Default TTL in seconds (default: 1 hour)
        """
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.default_ttl = default_ttl
        
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info("Redis cache repository initialized")
        except RedisError as e:
            logger.error(f"Redis connection failed: {e}")
            raise
    
    def _make_key(self, tariff_code: str, suffix: str = "latest") -> str:
        """Create Redis key for tariff code"""
        return f"tariff:{tariff_code}:{suffix}"
    
    def get(self, tariff_code: str) -> Optional[ScrapedData]:
        """Get scraped data from cache"""
        try:
            key = self._make_key(tariff_code)
            cached_json = self.redis_client.get(key)
            
            if cached_json:
                data_dict = json.loads(cached_json)
                scraped_data = self._dict_to_scraped_data(data_dict)
                logger.debug(f"Cache hit for {tariff_code}")
                return scraped_data
            
            logger.debug(f"Cache miss for {tariff_code}")
            return None
            
        except RedisError as e:
            logger.error(f"Redis error getting {tariff_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error deserializing cached data: {e}")
            return None
    
    def set(self, tariff_code: str, data: ScrapedData, ttl: Optional[int] = None) -> bool:
        """Set scraped data in cache"""
        try:
            key = self._make_key(tariff_code)
            ttl = ttl or self.default_ttl
            
            # Serialize data
            data_json = json.dumps(data.to_dict())
            
            # Store with TTL
            self.redis_client.setex(key, ttl, data_json)
            
            # Also store metadata (lightweight, longer TTL)
            metadata_key = self._make_key(tariff_code, "metadata")
            metadata = {
                "scraped_at": data.scraped_at.isoformat(),
                "version": getattr(data, 'version', 1),
                "hash": hash(str(data.to_dict()))
            }
            self.redis_client.setex(
                metadata_key,
                ttl * 2,  # Metadata lives twice as long
                json.dumps(metadata)
            )
            
            logger.debug(f"Cached {tariff_code} (TTL: {ttl}s)")
            return True
            
        except RedisError as e:
            logger.error(f"Redis error setting {tariff_code}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error serializing data for cache: {e}")
            return False
    
    def delete(self, tariff_code: str) -> bool:
        """Delete cached data"""
        try:
            key = self._make_key(tariff_code)
            metadata_key = self._make_key(tariff_code, "metadata")
            self.redis_client.delete(key, metadata_key)
            logger.debug(f"Deleted cache for {tariff_code}")
            return True
        except RedisError as e:
            logger.error(f"Redis error deleting {tariff_code}: {e}")
            return False
    
    def exists(self, tariff_code: str) -> bool:
        """Check if data exists in cache"""
        try:
            key = self._make_key(tariff_code)
            return self.redis_client.exists(key) > 0
        except RedisError as e:
            logger.error(f"Redis error checking existence: {e}")
            return False
    
    def get_metadata(self, tariff_code: str) -> Optional[Dict[str, Any]]:
        """Get lightweight metadata from cache"""
        try:
            metadata_key = self._make_key(tariff_code, "metadata")
            cached_metadata = self.redis_client.get(metadata_key)
            
            if cached_metadata:
                return json.loads(cached_metadata)
            return None
            
        except RedisError as e:
            logger.error(f"Redis error getting metadata: {e}")
            return None
    
    def set_monitoring_status(self, status: Dict[str, Any]) -> bool:
        """Set monitoring service status"""
        try:
            key = "monitoring:status"
            self.redis_client.set(key, json.dumps(status))
            return True
        except RedisError as e:
            logger.error(f"Redis error setting monitoring status: {e}")
            return False
    
    def get_monitoring_status(self) -> Optional[Dict[str, Any]]:
        """Get monitoring service status"""
        try:
            key = "monitoring:status"
            cached = self.redis_client.get(key)
            if cached:
                return json.loads(cached)
            return None
        except RedisError as e:
            logger.error(f"Redis error getting monitoring status: {e}")
            return None
    
    def clear_all(self) -> bool:
        """Clear all cached data (use with caution!)"""
        try:
            self.redis_client.flushdb()
            logger.warning("Cleared all cache data")
            return True
        except RedisError as e:
            logger.error(f"Redis error clearing cache: {e}")
            return False
    
    def _dict_to_scraped_data(self, data_dict: Dict[str, Any]) -> Optional[ScrapedData]:
        """Convert dictionary to ScrapedData entity"""
        try:
            from ...domain.entities import BasicInfo, SectionData, StructuredData, Metadata
            
            # Reconstruct BasicInfo
            basic_info_dict = data_dict.get('basic_info', {})
            basic_info = BasicInfo(
                tariff_code=basic_info_dict.get('tariff_code'),
                product_description=basic_info_dict.get('product_description'),
                effective_date=basic_info_dict.get('effective_date'),
                metadata=Metadata(basic_info_dict.get('metadata', {}))
            )
            
            # Create ScrapedData
            scraped_data = ScrapedData(
                tariff_code_searched=data_dict.get('tariff_code_searched', ''),
                basic_info=basic_info
            )
            
            # Add sections
            sections_dict = data_dict.get('sections', {})
            for section_name, section_dict in sections_dict.items():
                structured_dict = section_dict.get('structured_data', {})
                structured_data = StructuredData(
                    metadata=Metadata(structured_dict.get('metadata', {})),
                    tables=structured_dict.get('tables', []),
                    lists=structured_dict.get('lists', []),
                    section_specific=structured_dict.get('section_specific', {})
                )
                
                section_data = SectionData(
                    section_name=section_name,
                    structured_data=structured_data,
                    error=section_dict.get('error')
                )
                scraped_data.add_section(section_data)
            
            # Set timestamps
            if 'scraped_at' in data_dict:
                scraped_data.scraped_at = datetime.fromisoformat(data_dict['scraped_at'].replace('Z', '+00:00'))
            if 'scraping_duration_seconds' in data_dict:
                scraped_data.scraping_duration_seconds = data_dict['scraping_duration_seconds']
            
            return scraped_data
            
        except Exception as e:
            logger.error(f"Error converting dict to ScrapedData: {e}")
            return None

