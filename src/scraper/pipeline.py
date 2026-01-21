import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict

from .config import ScraperConfig, logger
from .scraper import ADILScraper

def scrape_single_code(hs_code: str, config: ScraperConfig) -> Dict:
    scraper = ADILScraper(config)
    try:
        result = scraper.scrape_hs_code(hs_code)
        return asdict(result)
    finally:
        scraper.close()

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
        
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                res = future.result()
                logger.info(f"✅ Finished Scraping {code}")
                yield res
            except Exception as e:
                logger.error(f"❌ Error on {code}: {e}")

    logger.info("Batch scraping sequence completed.")
