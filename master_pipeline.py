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

import argparse
from typing import Set
import psycopg2

def get_existing_hs_codes(dsn: str) -> Set[str]:
    """Fetch all hs10 codes currently in the database."""
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute("SELECT hs10 FROM hs_products")
        codes = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return codes
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch existing codes from DB: {e}")
        return set()

def run_pipeline(limit=None, force_etl=False):
    print(f"🚀 Starting Master Pipeline (Limit: {limit}, Force ETL: {force_etl})...")
    
    # Define paths relative to this script (project root)
    root_dir = Path(__file__).parent
    csv_input = root_dir / "Code Sh Import - Feuil.csv"
    
    # 0. Check for existing codes (Resume Capability)
    print("\n--- Phase 0: Checking for existing data ---")
    if force_etl:
        print("🔄 Force ETL enabled: Re-processing all codes.")
        existing_codes = set()
    else:
        existing_codes = get_existing_hs_codes(etl_processor.DSN)
        print(f"⏭️ Skipping {len(existing_codes)} already processed codes.")
    
    # 1. Database Initialization
    print("\n--- Phase 1: Database Initialization ---")
    init_db.init_db()
    
    # 2. Scraping & ETL Layer (Streaming)
    print("\n--- Phase 2: Integrated Scraping & ETL (Streaming) ---")
    import psycopg2
    conn = psycopg2.connect(etl_processor.DSN)
    try:
        count = 0
        # Iterate over results as they are yielded by the scraper
        for raw_record in scraper.main(csv_path=csv_input, skip_codes=existing_codes, save_to_file=False, limit=limit):
            # Process each record immediately
            etl_processor.process_single_record(raw_record, conn)
            count += 1
            
        if count == 0:
            print("⏭️ No new codes to process (all skipped or error).")
        else:
            print(f"\n✅ Total records processed: {count}")
            
    finally:
        conn.close()
    
    print("\n✅ Master Pipeline completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Pipeline for Scraping and ETL")
    parser.add_argument("--limit", type=int, help="Limit the number of HS codes to process")
    parser.add_argument("--force-etl", action="store_true", help="Re-process codes even if they exist in the database")
    args = parser.parse_args()
    
    run_pipeline(limit=args.limit, force_etl=args.force_etl)
