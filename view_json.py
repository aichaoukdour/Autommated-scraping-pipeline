"""
View scraped data as JSON
"""
import os
import sys
import json

# Set environment
os.environ['DATABASE_URL'] = 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6379/0'

from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
from src.infrastructure.storage.cleaned_repository import CleanedDataRepository

def view_json(tariff_code: str, pretty: bool = True, cleaned: bool = False):
    """View scraped or cleaned data as JSON"""
    if cleaned:
        # View cleaned data
        cleaned_repo = CleanedDataRepository()
        data = cleaned_repo.get(tariff_code)
        
        if not data:
            print(f"No cleaned data found for tariff code: {tariff_code}")
            return
        
        # Convert to dict
        data_dict = data.to_dict()
    else:
        # View raw scraped data
        db_repo = PostgreSQLRepository()
        data = db_repo.load_latest(tariff_code)
        
        if not data:
            print(f"No data found for tariff code: {tariff_code}")
            return
        
        # Convert to dict
        data_dict = data.to_dict()
    
    # Output as JSON
    if pretty:
        json_str = json.dumps(data_dict, indent=2, ensure_ascii=False, default=str)
    else:
        json_str = json.dumps(data_dict, ensure_ascii=False, default=str)
    
    print(json_str)

def view_all_codes_json():
    """View all tariff codes with their section counts"""
    db_repo = PostgreSQLRepository()
    
    # Get all codes from database
    conn = db_repo._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    tariff_code,
                    version,
                    scraped_at,
                    data->'basic_info'->>'product_description' as product,
                    jsonb_object_keys(data->'sections') as section_name
                FROM scraped_data
                WHERE (tariff_code, version) IN (
                    SELECT tariff_code, MAX(version)
                    FROM scraped_data
                    GROUP BY tariff_code
                )
                ORDER BY tariff_code, section_name
            """)
            rows = cur.fetchall()
            
            # Group by tariff code
            codes_data = {}
            for row in rows:
                code = row[0]
                if code not in codes_data:
                    codes_data[code] = {
                        'tariff_code': code,
                        'version': row[1],
                        'scraped_at': str(row[2]),
                        'product': row[3],
                        'sections': []
                    }
                codes_data[code]['sections'].append(row[4])
            
            # Convert to list
            result = list(codes_data.values())
            
            # Output as JSON
            json_str = json.dumps(result, indent=2, ensure_ascii=False)
            print(json_str)
            
    finally:
        db_repo._return_connection(conn)

def view_stats_json():
    """View statistics as JSON"""
    db_repo = PostgreSQLRepository()
    
    conn = db_repo._get_connection()
    try:
        with conn.cursor() as cur:
            # Get statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT tariff_code) as unique_codes,
                    MAX(scraped_at) as latest_scrape,
                    MIN(scraped_at) as first_scrape
                FROM scraped_data
            """)
            stats_row = cur.fetchone()
            
            # Get section counts per code
            cur.execute("""
                SELECT 
                    tariff_code,
                    COUNT(DISTINCT jsonb_object_keys(data->'sections')) as section_count
                FROM scraped_data
                WHERE (tariff_code, version) IN (
                    SELECT tariff_code, MAX(version)
                    FROM scraped_data
                    GROUP BY tariff_code
                )
                GROUP BY tariff_code
                ORDER BY tariff_code
            """)
            section_counts = []
            for row in cur.fetchall():
                section_counts.append({
                    'tariff_code': row[0],
                    'section_count': row[1]
                })
            
            # Build stats object
            stats = {
                'total_records': stats_row[0],
                'unique_codes': stats_row[1],
                'latest_scrape': str(stats_row[2]) if stats_row[2] else None,
                'first_scrape': str(stats_row[3]) if stats_row[3] else None,
                'section_counts': section_counts
            }
            
            # Output as JSON
            json_str = json.dumps(stats, indent=2, ensure_ascii=False)
            print(json_str)
            
    finally:
        db_repo._return_connection(conn)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python view_json.py view <tariff_code>           - View raw data for one code")
        print("  python view_json.py cleaned <tariff_code>        - View cleaned data for one code")
        print("  python view_json.py list                         - List all codes with sections")
        print("  python view_json.py stats                        - View statistics")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "view" and len(sys.argv) > 2:
        view_json(sys.argv[2], pretty=True, cleaned=False)
    elif command == "cleaned" and len(sys.argv) > 2:
        view_json(sys.argv[2], pretty=True, cleaned=True)
    elif command == "list":
        view_all_codes_json()
    elif command == "stats":
        view_stats_json()
    else:
        print("Invalid command. Use: view <code>, cleaned <code>, list, or stats")

