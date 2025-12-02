"""
Script to view scraped data from database
"""

import os
import json
import sys

# Set environment
os.environ['DATABASE_URL'] = 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6379/0'

from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
from src.infrastructure.storage.redis_cache_repository import RedisCacheRepository

def view_latest_data(tariff_code: str):
    """View latest scraped data for a tariff code"""
    db_repo = PostgreSQLRepository()
    
    print(f"\n{'='*70}")
    print(f"Latest Data for Tariff Code: {tariff_code}")
    print(f"{'='*70}\n")
    
    # Get from database
    data = db_repo.load_latest(tariff_code)
    
    if data:
        print(f"Tariff Code: {data.tariff_code_searched}")
        print(f"Scraped At: {data.scraped_at}")
        if data.scraping_duration_seconds:
            print(f"Duration: {data.scraping_duration_seconds:.2f} seconds")
        print(f"\nBasic Info:")
        print(f"  - Code: {data.basic_info.tariff_code}")
        print(f"  - Description: {data.basic_info.product_description}")
        print(f"  - Effective Date: {data.basic_info.effective_date}")
        print(f"\nSections ({len(data.sections)}):")
        for section_name, section_data in data.sections.items():
            print(f"  - {section_name}")
            if section_data.structured_data.tables:
                print(f"    Tables: {len(section_data.structured_data.tables)}")
            if section_data.structured_data.lists:
                print(f"    Lists: {len(section_data.structured_data.lists)}")
    else:
        print(f"No data found for {tariff_code}")

def view_all_codes():
    """View all monitored codes and their status"""
    db_repo = PostgreSQLRepository()
    
    print(f"\n{'='*70}")
    print("All Scraped Tariff Codes")
    print(f"{'='*70}\n")
    
    # Get all codes from database
    conn = db_repo._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    tariff_code,
                    COUNT(*) as versions,
                    MAX(scraped_at) as last_scraped,
                    MAX(version) as latest_version
                FROM scraped_data
                GROUP BY tariff_code
                ORDER BY last_scraped DESC
            """)
            rows = cur.fetchall()
            
            if rows:
                print(f"{'Tariff Code':<20} {'Versions':<10} {'Latest Version':<15} {'Last Scraped':<25}")
                print("-" * 70)
                for row in rows:
                    print(f"{row[0]:<20} {row[1]:<10} {row[3]:<15} {str(row[2])[:25]:<25}")
            else:
                print("No scraped data found")
    finally:
        db_repo._return_connection(conn)

def export_to_json(tariff_code: str, output_file: str = None):
    """Export scraped data to JSON file"""
    db_repo = PostgreSQLRepository()
    
    data = db_repo.load_latest(tariff_code)
    
    if not data:
        print(f"No data found for {tariff_code}")
        return
    
    if not output_file:
        output_file = f"data_{tariff_code}_{data.scraped_at.strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data.to_dict(), f, indent=2, ensure_ascii=False)
    
    print(f"Data exported to: {output_file}")

def view_cache_status(tariff_code: str):
    """View cache status for a tariff code"""
    cache_repo = RedisCacheRepository()
    
    print(f"\n{'='*70}")
    print(f"Cache Status for: {tariff_code}")
    print(f"{'='*70}\n")
    
    exists = cache_repo.exists(tariff_code)
    print(f"In Cache: {exists}")
    
    if exists:
        metadata = cache_repo.get_metadata(tariff_code)
        if metadata:
            print(f"Scraped At: {metadata.get('scraped_at')}")
            print(f"Version: {metadata.get('version')}")
            print(f"Hash: {metadata.get('hash')}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python view_data.py list                    - List all codes")
        print("  python view_data.py view <code>              - View latest data")
        print("  python view_data.py export <code> [file]     - Export to JSON")
        print("  python view_data.py cache <code>             - View cache status")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "list":
        view_all_codes()
    elif command == "view" and len(sys.argv) > 2:
        view_latest_data(sys.argv[2])
    elif command == "export" and len(sys.argv) > 2:
        output_file = sys.argv[3] if len(sys.argv) > 3 else None
        export_to_json(sys.argv[2], output_file)
    elif command == "cache" and len(sys.argv) > 2:
        view_cache_status(sys.argv[2])
    else:
        print("Invalid command. Use: list, view <code>, export <code> [file], or cache <code>")

