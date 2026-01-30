import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict

from .config import ScraperConfig, logger
from .scraper import ADILScraper

import threading

_thread_local = threading.local()

def get_scraper(config: ScraperConfig) -> ADILScraper:
    if not hasattr(_thread_local, "scraper") or not _thread_local.scraper.is_alive():
        _thread_local.scraper = ADILScraper(config)
    return _thread_local.scraper

def scrape_single_code(hs_code: str, config: ScraperConfig) -> Dict:
    scraper = get_scraper(config)
    try:
        # Periodic refresh to keep browser stable over 13k codes
        if scraper.codes_processed >= 100:
            logger.info(f"Thread reaching 100 codes. Refreshing browser session...")
            scraper.restart_driver()
            
        result = scraper.scrape_hs_code(hs_code)
        return asdict(result)
    except Exception as e:
        logger.error(f"Error in thread scraping {hs_code}: {e}")
        # On structural errors, a restart is safer
        try:
            scraper.restart_driver()
        except:
            pass
        raise e

def main(
    csv_path: Optional[Path] = None, 
    output_dir: Path = Path("."), 
    skip_codes: Optional[Set[str]] = None, 
    save_to_file: bool = True,
    limit: Optional[int] = None
):
    """Main execution function compatible with master pipeline"""
    config = ScraperConfig(headless=True, max_workers=3)
    if csv_path is None:
        csv_path = Path("Code Sh Import - Feuil.csv")
    
    codes = []
    if csv_path.exists():
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            codes = [row['hs_code'].strip() for row in reader if row.get('hs_code')]
    else:
        logger.error(f"CSV file not found: {csv_path}")
        return []

    if skip_codes:
        initial_count = len(codes)
        codes = [c for c in codes if c not in skip_codes]
        logger.info(f"Skipping {initial_count - len(codes)} already processed codes")

    if limit:
        codes = codes[:limit]
        logger.info(f"Limiting to first {limit} codes")

    if not codes:
        logger.info("No codes to process.")
        return []

    logger.info(f"Starting batch process for {len(codes)} codes (Streaming Mode)...")
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_map = {executor.submit(scrape_single_code, code, config): code for code in codes}
        
        try:
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    res = future.result()
                    logger.info(f"✅ Finished Scraping {code}")
                    yield res
                except Exception as e:
                    logger.error(f"❌ Error on {code}: {e}")
        finally:
            # Cleanup scrapers in all threads
            logger.info("Cleaning up shared browser instances...")
            def cleanup():
                if hasattr(_thread_local, "scraper"):
                    _thread_local.scraper.close()
            
            # Submit cleanup tasks to each worker thread
            for _ in range(config.max_workers):
                executor.submit(cleanup)

    logger.info("Batch scraping sequence completed.")
