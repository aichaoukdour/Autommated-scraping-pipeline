"""
ETL Pipeline - Extract, Transform, Load
Transforms raw scraped data into cleaned data
"""

import logging
from typing import List, Optional

from ..domain.entities import ScrapedData
from ..domain.cleaned_entities import CleanedData
from ..infrastructure.storage.postgresql_repository import PostgreSQLRepository
from ..infrastructure.storage.cleaned_repository import CleanedDataRepository
from .transformers import DataTransformer
import psycopg2

logger = logging.getLogger(__name__)


class ETLPipeline:
    """ETL Pipeline for transforming raw data to cleaned data"""
    
    def __init__(self):
        """Initialize ETL pipeline"""
        self.raw_repo = PostgreSQLRepository()
        self.cleaned_repo = CleanedDataRepository()
        self.transformer = DataTransformer()
    
    def transform_all(self, tariff_codes: Optional[List[str]] = None) -> int:
        """Transform all raw data to cleaned data"""
        if tariff_codes:
            codes_to_process = tariff_codes
        else:
            # Get all tariff codes from raw data
            codes_to_process = self.raw_repo.list_all()
        
        logger.info(f"Starting ETL for {len(codes_to_process)} tariff codes")
        
        success_count = 0
        for tariff_code in codes_to_process:
            try:
                # Extract: Get raw data
                raw_data = self.raw_repo.load_latest(tariff_code)
                if not raw_data:
                    logger.warning(f"No raw data found for {tariff_code}")
                    continue
                
                # Transform: Clean the data
                cleaned_data = self.transformer.transform(raw_data)
                # Get version from database
                import os
                conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT MAX(version) FROM scraped_data WHERE tariff_code = %s",
                            (tariff_code,)
                        )
                        version = cur.fetchone()[0]
                        cleaned_data.source_version = version
                finally:
                    conn.close()
                
                # Load: Save cleaned data
                if self.cleaned_repo.save(cleaned_data):
                    success_count += 1
                    logger.info(f"✓ Transformed {tariff_code}")
                else:
                    logger.error(f"✗ Failed to save cleaned data for {tariff_code}")
                    
            except Exception as e:
                logger.error(f"Error processing {tariff_code}: {e}", exc_info=True)
        
        logger.info(f"ETL completed: {success_count}/{len(codes_to_process)} successful")
        return success_count
    
    def transform_one(self, tariff_code: str) -> bool:
        """Transform a single tariff code"""
        return self.transform_all([tariff_code]) == 1

