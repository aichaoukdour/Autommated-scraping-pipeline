# master_pipeline.py

import sys
import os
from pathlib import Path

# Add src and src/etl to path so we can import modules
sys.path.append(str(Path(__file__).parent / "src"))
sys.path.append(str(Path(__file__).parent / "src" / "etl"))

import scraper
import init_db
from etl import main as etl_main
from etl import resume_utils

def run_pipeline(limit=None):
    print(f"🚀 Starting Master Pipeline (Limit: {limit})...")
    
    # Define paths relative to this script (project root)
    root_dir = Path(__file__).parent
    csv_input = root_dir / "Code Sh Import - Feuil.csv"
    scraper_output_dir = root_dir / "src"
    detailed_json = scraper_output_dir / "adil_detailed.json"
    
    # 0. Check for existing codes (Resume Capability)
    print("\n--- Phase 0: Checking for existing data ---")
    existing_codes = resume_utils.get_existing_hs_codes(etl_main.DSN)
    
    # 1. Database Initialization
    print("\n--- Phase 1: Database Initialization ---")
    init_db.init_db()
    
    # 2. Scraping & ETL Layer (Streaming)
    print("\n--- Phase 2: Integrated Scraping & ETL (Streaming) ---")
    import psycopg2
    conn = psycopg2.connect(etl_main.DSN)
    try:
        count = 0
        # Iterate over results as they are yielded by the scraper
        for raw_record in scraper.main(csv_path=csv_input, skip_codes=existing_codes, save_to_file=False, limit=limit):
            # Process each record immediately
            etl_main.process_single_record(raw_record, conn)
            count += 1
            
        if count == 0:
            print("⏭️ No new codes to process (all skipped or error).")
        else:
            print(f"\n✅ Total records processed: {count}")
            
    finally:
        conn.close()
    
    print("\n✅ Master Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
