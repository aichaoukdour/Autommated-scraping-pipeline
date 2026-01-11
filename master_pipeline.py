# master_pipeline.py

import sys
import os
from pathlib import Path

# Add src and src/etl to path so we can import modules
sys.path.append(str(Path(__file__).parent / "src"))
sys.path.append(str(Path(__file__).parent / "src" / "etl"))

import scraper
from etl import main as etl_main
from etl import resume_utils

def run_pipeline():
    print("🚀 Starting Master Pipeline...")
    
    # Define paths relative to this script (project root)
    root_dir = Path(__file__).parent
    csv_input = root_dir / "Code Sh Import - Feuil.csv"
    scraper_output_dir = root_dir / "src"
    detailed_json = scraper_output_dir / "adil_detailed.json"
    
    # 0. Check for existing codes (Resume Capability)
    print("\n--- Phase 0: Checking for existing data ---")
    existing_codes = resume_utils.get_existing_hs_codes(etl_main.DSN)
    
    # 1. Scraping Layer
    print("\n--- Phase 1: Scraping (Direct Streaming) ---")
    raw_data = scraper.main(csv_path=csv_input, output_dir=scraper_output_dir, skip_codes=existing_codes, save_to_file=False)
    
    # 2. ETL Layer
    print("\n--- Phase 2: ETL (Extract, Transform, Load) ---")
    if not raw_data:
        print("⏭️ No new data to process (either skipped or error).")
    else:
        etl_main.process_data(raw_data, etl_main.DSN)
    
    print("\n✅ Master Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
