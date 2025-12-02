"""
Quick script to trigger a scrape for a tariff code
"""
import os
import sys

# Set environment
os.environ['DATABASE_URL'] = 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6379/0'
os.environ['PLAYWRIGHT_HEADLESS'] = 'false'

from src.domain.entities import TariffCode
from src.infrastructure.scraping.playwright_repository import PlaywrightScrapingRepository
from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository

def scrape_tariff_code(code: str):
    """Scrape a single tariff code"""
    print(f"\n{'='*70}")
    print(f"Scraping Tariff Code: {code}")
    print(f"{'='*70}\n")
    
    # Initialize repositories
    scraper = PlaywrightScrapingRepository()
    db_repo = PostgreSQLRepository()
    
    # Scrape
    print("Starting scrape...")
    print("(This may take a minute - checking all sections in sidebar)...")
    scraped_data = scraper.scrape(TariffCode(code))
    
    if scraped_data:
        print(f"\n[OK] Scraping successful!")
        print(f"  - Sections found: {len(scraped_data.sections)}")
        print(f"  - Section names: {', '.join(scraped_data.sections.keys())}")
        
        # Save to database
        print("\nSaving to database...")
        db_repo.save(scraped_data)
        print("[OK] Data saved to database")
        
        # Show summary
        print(f"\n{'='*70}")
        print("Summary:")
        print(f"  Tariff Code: {scraped_data.tariff_code_searched}")
        print(f"  Product: {scraped_data.basic_info.product_description}")
        print(f"  Sections: {len(scraped_data.sections)}")
        print(f"  Scraped At: {scraped_data.scraped_at}")
        print(f"{'='*70}\n")
        
        return True
    else:
        print("\n[ERROR] Scraping failed - no data returned")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scrape_now.py <tariff_code>")
        print("Example: python scrape_now.py 0804100000")
        sys.exit(1)
    
    code = sys.argv[1]
    success = scrape_tariff_code(code)
    sys.exit(0 if success else 1)

