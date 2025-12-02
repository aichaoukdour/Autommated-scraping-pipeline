"""
PostgreSQL repository implementation
"""

import json
import os
import hashlib
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

from ...domain.entities import ScrapedData
from ...domain.repositories import FileRepository

logger = logging.getLogger(__name__)


class PostgreSQLRepository(FileRepository):
    """PostgreSQL implementation of FileRepository"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize PostgreSQL repository
        
        Args:
            database_url: PostgreSQL connection string
                         Defaults to DATABASE_URL environment variable
        """
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable or database_url parameter required")
        
        # Create connection pool
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=self.database_url
        )
        
        # Test connection
        self._test_connection()
        logger.info("PostgreSQL repository initialized")
    
    def _test_connection(self):
        """Test database connection"""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logger.info("PostgreSQL connection successful")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
        finally:
            self.pool.putconn(conn)
    
    def _get_connection(self):
        """Get connection from pool"""
        return self.pool.getconn()
    
    def _return_connection(self, conn):
        """Return connection to pool"""
        self.pool.putconn(conn)
    
    def _calculate_hash(self, data: ScrapedData) -> str:
        """Calculate SHA256 hash of the data for deduplication"""
        # Convert to dict and sort keys for consistent hashing
        data_dict = data.to_dict()
        # Remove scraped_at and version from hash calculation (they change but data might be same)
        hash_dict = {
            'tariff_code_searched': data_dict.get('tariff_code_searched'),
            'basic_info': data_dict.get('basic_info'),
            'sections': data_dict.get('sections')
        }
        # Convert to JSON with sorted keys for consistent hashing
        json_str = json.dumps(hash_dict, sort_keys=True, ensure_ascii=False)
        # Calculate SHA256 hash
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def _hash_exists(self, conn, tariff_code: str, data_hash: str) -> bool:
        """Check if hash already exists for this tariff code"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM scraped_data WHERE tariff_code = %s AND data_hash = %s",
                (tariff_code, data_hash)
            )
            count = cur.fetchone()[0]
            return count > 0
    
    def save(self, data: ScrapedData, filepath: str = None) -> bool:
        """
        Save scraped data to database with deduplication
        
        Returns:
            bool: True if saved (new data), False if duplicate or error
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                # Calculate hash of the data
                data_hash = self._calculate_hash(data)
                
                # Check if this exact data already exists
                if self._hash_exists(conn, data.tariff_code_searched, data_hash):
                    logger.info(f"Duplicate data detected for {data.tariff_code_searched} (hash: {data_hash[:16]}...). Skipping save.")
                    return False
                
                # Get current version
                cur.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 FROM scraped_data WHERE tariff_code = %s",
                    (data.tariff_code_searched,)
                )
                version = cur.fetchone()[0]
                
                # Insert new version with hash
                cur.execute(
                    """
                    INSERT INTO scraped_data (tariff_code, data, scraped_at, version, data_hash)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        data.tariff_code_searched,
                        json.dumps(data.to_dict()),
                        data.scraped_at,
                        version,
                        data_hash
                    )
                )
                conn.commit()
                logger.info(f"Saved {data.tariff_code_searched} version {version} to PostgreSQL (hash: {data_hash[:16]}...)")
                return True
                
        except psycopg2.IntegrityError as e:
            # Handle unique constraint violation (tariff_code + data_hash)
            if 'idx_scraped_data_tariff_hash' in str(e):
                logger.info(f"Duplicate data detected for {data.tariff_code_searched} (unique constraint). Skipping save.")
                if conn:
                    conn.rollback()
                return False
            else:
                logger.error(f"Integrity error saving to PostgreSQL: {e}")
                if conn:
                    conn.rollback()
                return False
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self._return_connection(conn)
    
    def load(self, filepath: str) -> Optional[ScrapedData]:
        """Load latest scraped data for a tariff code"""
        # filepath is treated as tariff_code
        tariff_code = filepath
        return self.load_latest(tariff_code)
    
    def get_hash_info(self, tariff_code: str) -> Optional[Dict[str, Any]]:
        """Get hash information for a tariff code"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT data_hash, version, scraped_at, 
                           (SELECT COUNT(*) FROM scraped_data WHERE tariff_code = %s) as total_versions
                    FROM scraped_data
                    WHERE tariff_code = %s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    (tariff_code, tariff_code)
                )
                row = cur.fetchone()
                if row:
                    return {
                        'data_hash': row['data_hash'],
                        'version': row['version'],
                        'scraped_at': row['scraped_at'],
                        'total_versions': row['total_versions']
                    }
                return None
        except Exception as e:
            logger.error(f"Error getting hash info: {e}")
            return None
        finally:
            if conn:
                self._return_connection(conn)
    
    def load_latest(self, tariff_code: str) -> Optional[ScrapedData]:
        """Load latest version of scraped data"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT data, scraped_at, version, data_hash
                    FROM scraped_data
                    WHERE tariff_code = %s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    (tariff_code,)
                )
                row = cur.fetchone()
                
                if row:
                    data_dict = row['data']
                    scraped_data = self._dict_to_scraped_data(data_dict)
                    if scraped_data:
                        scraped_data.scraped_at = row['scraped_at']
                    return scraped_data
                return None
                
        except Exception as e:
            logger.error(f"Error loading from PostgreSQL: {e}")
            return None
        finally:
            if conn:
                self._return_connection(conn)
    
    def load_history(self, tariff_code: str, limit: int = 10) -> List[ScrapedData]:
        """Load version history for a tariff code"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT data, scraped_at, version
                    FROM scraped_data
                    WHERE tariff_code = %s
                    ORDER BY version DESC
                    LIMIT %s
                    """,
                    (tariff_code, limit)
                )
                rows = cur.fetchall()
                
                results = []
                for row in rows:
                    data = self._dict_to_scraped_data(row['data'])
                    if data:
                        results.append(data)
                return results
                
        except Exception as e:
            logger.error(f"Error loading history from PostgreSQL: {e}")
            return []
        finally:
            if conn:
                self._return_connection(conn)
    
    def log_change(
        self,
        tariff_code: str,
        change_type: str,
        old_data: Optional[ScrapedData],
        new_data: ScrapedData,
        changes_summary: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Log a data change"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO data_changes 
                    (tariff_code, change_type, old_data, new_data, changes_summary)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        tariff_code,
                        change_type,
                        json.dumps(old_data.to_dict()) if old_data else None,
                        json.dumps(new_data.to_dict()),
                        json.dumps(changes_summary) if changes_summary else None
                    )
                )
                conn.commit()
                logger.debug(f"Logged {change_type} change for {tariff_code}")
                return True
                
        except Exception as e:
            logger.error(f"Error logging change: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self._return_connection(conn)
    
    def log_monitoring_activity(
        self,
        tariff_code: str,
        action: str,
        status: str,
        duration_ms: Optional[int] = None,
        message: Optional[str] = None
    ) -> bool:
        """Log monitoring activity"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring_log 
                    (tariff_code, action, status, duration_ms, message)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (tariff_code, action, status, duration_ms, message)
                )
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error logging monitoring activity: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self._return_connection(conn)
    
    def load_tariff_codes(self, filepath: str) -> List[str]:
        """Load tariff codes from monitored_codes table"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tariff_code
                    FROM monitored_codes
                    WHERE enabled = TRUE
                    ORDER BY priority DESC, tariff_code
                    """
                )
                rows = cur.fetchall()
                return [row[0] for row in rows]
                
        except Exception as e:
            logger.error(f"Error loading tariff codes: {e}")
            return []
        finally:
            if conn:
                self._return_connection(conn)
    
    def add_monitored_code(
        self,
        tariff_code: str,
        interval_minutes: int = 60,
        priority: int = 0
    ) -> bool:
        """Add a tariff code to monitoring"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitored_codes 
                    (tariff_code, interval_minutes, priority)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tariff_code) 
                    DO UPDATE SET 
                        enabled = TRUE,
                        interval_minutes = EXCLUDED.interval_minutes,
                        priority = EXCLUDED.priority,
                        updated_at = NOW()
                    """,
                    (tariff_code, interval_minutes, priority)
                )
                conn.commit()
                logger.info(f"Added monitored code: {tariff_code}")
                return True
                
        except Exception as e:
            logger.error(f"Error adding monitored code: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self._return_connection(conn)
    
    def save_all(self, data_list: List[ScrapedData], filepath: str = None) -> bool:
        """Save multiple scraped data records"""
        success_count = 0
        for data in data_list:
            if self.save(data):
                success_count += 1
        return success_count == len(data_list)
    
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

