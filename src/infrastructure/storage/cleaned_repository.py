"""
Repository for storing cleaned/transformed data
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ...domain.cleaned_entities import CleanedData

logger = logging.getLogger(__name__)


class CleanedDataRepository:
    """Repository for cleaned/transformed data"""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize with database URL"""
        import os
        self.database_url = database_url or os.environ.get('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable or database_url parameter required")
    
    def save(self, cleaned_data: CleanedData) -> bool:
        """Save cleaned data to database"""
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            
            # Convert to dict with proper serialization
            data_dict = self._serialize_for_json(cleaned_data.to_dict())
            
            # Insert into cleaned_data table
            cur.execute("""
                INSERT INTO cleaned_data (
                    tariff_code, data, cleaned_at, source_version, source_scraped_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tariff_code) 
                DO UPDATE SET 
                    data = EXCLUDED.data,
                    cleaned_at = EXCLUDED.cleaned_at,
                    source_version = EXCLUDED.source_version,
                    source_scraped_at = EXCLUDED.source_scraped_at
            """, (
                cleaned_data.tariff_code,
                json.dumps(data_dict),
                cleaned_data.cleaned_at,
                cleaned_data.source_version,
                cleaned_data.source_scraped_at
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Saved cleaned data for {cleaned_data.tariff_code}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving cleaned data: {e}")
            return False
    
    def get(self, tariff_code: str) -> Optional[CleanedData]:
        """Get cleaned data for a tariff code"""
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT data, cleaned_at, source_version, source_scraped_at
                FROM cleaned_data
                WHERE tariff_code = %s
                ORDER BY cleaned_at DESC
                LIMIT 1
            """, (tariff_code,))
            
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                data_dict = row[0]
                # Reconstruct CleanedData from dict
                return self._dict_to_cleaned_data(data_dict, tariff_code)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting cleaned data: {e}")
            return None
    
    def list_all(self) -> List[str]:
        """List all tariff codes with cleaned data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT DISTINCT tariff_code
                FROM cleaned_data
                ORDER BY tariff_code
            """)
            
            codes = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            return codes
            
        except Exception as e:
            logger.error(f"Error listing cleaned data: {e}")
            return []
    
    def _serialize_for_json(self, obj: Any) -> Any:
        """Recursively serialize objects for JSON (handles Decimal, datetime, etc.)"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        else:
            return obj
    
    def _dict_to_cleaned_data(self, data_dict: Dict[str, Any], tariff_code: str) -> CleanedData:
        """Convert dictionary to CleanedData entity"""
        from ...domain.cleaned_entities import CleanedSection, CleanedTable, CleanedTableRow
        
        cleaned = CleanedData(
            tariff_code=tariff_code,
            product_description=data_dict.get('product_description'),
            effective_date=datetime.fromisoformat(data_dict['effective_date']) if data_dict.get('effective_date') else None,
            metadata=data_dict.get('metadata', {}),
            cleaned_at=datetime.fromisoformat(data_dict['cleaned_at']),
            source_version=data_dict.get('source_version'),
            source_scraped_at=datetime.fromisoformat(data_dict['source_scraped_at']) if data_dict.get('source_scraped_at') else None
        )
        
        # Reconstruct sections
        sections_dict = data_dict.get('sections', {})
        for section_name, section_dict in sections_dict.items():
            cleaned_section = CleanedSection(
                section_name=section_name,
                section_type=section_dict.get('section_type', 'Other'),
                metadata=section_dict.get('metadata', {}),
                raw_errors=section_dict.get('raw_errors')
            )
            
            # Reconstruct tables
            for table_dict in section_dict.get('tables', []):
                cleaned_table = CleanedTable(
                    name=table_dict['name'],
                    headers=table_dict['headers'],
                    rows=[],  # Initialize empty rows
                    metadata=table_dict.get('metadata', {})
                )
                
                for row_dict in table_dict.get('rows', []):
                    cleaned_row = CleanedTableRow()
                    cleaned_row.row_data = row_dict
                    cleaned_table.rows.append(cleaned_row)
                
                cleaned_section.tables.append(cleaned_table)
            
            cleaned.add_section(cleaned_section)
        
        return cleaned

