"""
Check hash status for tariff codes
"""
import os
import sys

os.environ['DATABASE_URL'] = 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6379/0'

from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository

def check_hash(tariff_code: str):
    """Check hash information for a tariff code"""
    repo = PostgreSQLRepository()
    
    hash_info = repo.get_hash_info(tariff_code)
    
    if hash_info:
        print(f"\n{'='*70}")
        print(f"Hash Information for: {tariff_code}")
        print(f"{'='*70}\n")
        print(f"Hash: {hash_info['data_hash']}")
        print(f"Version: {hash_info['version']}")
        print(f"Total Versions: {hash_info['total_versions']}")
        print(f"Last Scraped: {hash_info['scraped_at']}")
        print(f"\n{'='*70}\n")
    else:
        print(f"No data found for tariff code: {tariff_code}")

def list_all_hashes():
    """List all tariff codes with their hashes"""
    repo = PostgreSQLRepository()
    
    conn = repo._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    tariff_code,
                    data_hash,
                    version,
                    scraped_at,
                    COUNT(*) OVER (PARTITION BY tariff_code) as total_versions
                FROM scraped_data
                WHERE (tariff_code, version) IN (
                    SELECT tariff_code, MAX(version)
                    FROM scraped_data
                    GROUP BY tariff_code
                )
                ORDER BY tariff_code
            """)
            
            rows = cur.fetchall()
            
            if rows:
                print(f"\n{'='*70}")
                print("All Tariff Codes with Hashes")
                print(f"{'='*70}\n")
                print(f"{'Tariff Code':<20} {'Hash (first 16 chars)':<20} {'Version':<10} {'Total Versions':<15} {'Last Scraped':<25}")
                print("-" * 70)
                for row in rows:
                    hash_str = row[1][:16] + "..." if row[1] else "NULL"
                    print(f"{row[0]:<20} {hash_str:<20} {row[2]:<10} {row[4]:<15} {str(row[3])[:25]:<25}")
                print(f"\n{'='*70}\n")
            else:
                print("No data found")
    finally:
        repo._return_connection(conn)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        list_all_hashes()
    elif sys.argv[1] == "list":
        list_all_hashes()
    elif len(sys.argv) > 1:
        check_hash(sys.argv[1])
    else:
        print("Usage:")
        print("  python check_hash.py              - List all codes with hashes")
        print("  python check_hash.py list          - List all codes with hashes")
        print("  python check_hash.py <tariff_code> - Check hash for specific code")

