# master_pipeline.py

import sys
import os
from pathlib import Path

# Add src and src/etl to path so we can import modules
sys.path.append(str(Path(__file__).parent / "src"))
sys.path.append(str(Path(__file__).parent / "src" / "etl"))

import scraper
import init_db
from etl import processor as etl_processor
from scraper.config import logger, ConnectionManager, ScraperConfig

import argparse
from typing import Set
import psycopg2

def get_codes_to_skip(dsn: str, refresh_days: int = 25) -> Set[str]:
    """
    Fetch hs10 codes that were updated RECENTLY.
    If a code was updated > refresh_days ago, we DON'T skip it (so we can get monthly updates).
    """
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        # Only skip codes updated in the last X days
        query = f"SELECT hs10 FROM hs_products WHERE updated_at > now() - interval '{refresh_days} days'"
        cur.execute(query)
        codes = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return codes
    except Exception as e:
        logger.warning(f"Could not fetch sync state from DB (starting fresh): {e}")
        return set()

def run_pipeline(limit=None, force_etl=False):
    logger.info(f"Starting Master Pipeline (Limit: {limit}, Force ETL: {force_etl})...")
    
    # Initialize Core Config
    config = ScraperConfig()
    
    # Define paths relative to this script (project root)
    root_dir = Path(__file__).parent
    csv_input = root_dir / "Code Sh Import - Feuil.csv"
    
    # 0. Sync/Resume Logic (Monthly Refresh)
    logger.info("Phase 0: Synchronizing Delta (Monthly Refresh Check)")
    if force_etl:
        logger.info("Force ETL enabled: Re-processing EVERYTHING.")
        codes_to_skip = set()
    else:
        codes_to_skip = get_codes_to_skip(config.db_dsn)
        logger.info(f"Skipping {len(codes_to_skip)} codes updated in the last 25 days.")
    
    # 1. Database Initialization
    logger.info("Phase 1: Database Initialization")
    init_db.init_db()
    
    # 2. Initialize Connection Pool
    ConnectionManager.initialize_pool(config)
    
    # 3. Scraping & ETL Layer (Streaming)
    logger.info("Phase 2: Integrated Scraping & ETL (Streaming)")
    
    try:
        count = 0
        batch_size = 50
        
        # Use simple try/except inside the loop for record-level resilience
        # Connection is acquired from the pool for each batch or record
        with ConnectionManager.get_connection() as conn:
            conn.autocommit = False
            
            # Iterate over results as they are yielded by the scraper
            for raw_record in scraper.main(csv_path=csv_input, skip_codes=codes_to_skip, save_to_file=False, limit=limit):
                try:
                    # Process record
                    etl_processor.process_single_record(raw_record, conn, commit_on_success=False)
                    count += 1
                    
                    # Periodic batch commit
                    if count % batch_size == 0:
                        conn.commit()
                        logger.info(f"💾 Batch committed at {count} records.")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error processing record: {e}")
                    # If the connection itself is dead, get_connection will handle it next time
                    continue
                        
            # Final commit
            conn.commit()
            
        if count == 0:
            logger.info("No new codes to process (all skipped or error).")
        else:
            logger.info(f"Total records processed: {count}")
            
    except Exception as e:
        logger.error(f"Critical Pipeline Failure: {e}")
        raise e
    finally:
        ConnectionManager.close_all()
    
    logger.info("Master Pipeline completed successfully!")
    config.send_notification(f"🚀 HS Code Sync Complete! Total: {count} records processed successfully.")
    
    # 4. Generate Health Report
    try:
        from generate_report import generate_health_report
        generate_health_report()
    except Exception as e:
        logger.warning(f"Could not generate final health report: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Pipeline for Scraping and ETL")
    parser.add_argument("--limit", type=int, help="Limit the number of HS codes to process")
    parser.add_argument("--force-etl", action="store_true", help="Re-process codes even if they exist in the database")
    args = parser.parse_args()
    
    run_pipeline(limit=args.limit, force_etl=args.force_etl)
