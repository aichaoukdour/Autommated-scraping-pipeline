"""Run ETL pipeline to clean raw data"""

import os
import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

os.environ['DATABASE_URL'] = os.environ.get('DATABASE_URL', 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper')
os.environ['REDIS_URL'] = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')

from src.application.etl_pipeline import ETLPipeline

def main():
    """Run ETL pipeline"""
    pipeline = ETLPipeline()
    
    if len(sys.argv) > 1:
        # Transform specific code(s)
        codes = sys.argv[1:]
        print(f"Transforming {len(codes)} tariff code(s)...")
        success = pipeline.transform_all(codes)
        print(f"\nCompleted: {success}/{len(codes)} successful")
    else:
        # Transform all
        print("Transforming all raw data...")
        success = pipeline.transform_all()
        print(f"\nCompleted: {success} successful")

if __name__ == "__main__":
    main()


