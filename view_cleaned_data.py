"""View cleaned/transformed data"""

import os
import sys
import json

os.environ['DATABASE_URL'] = os.environ.get('DATABASE_URL', 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper')
os.environ['REDIS_URL'] = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')

from src.infrastructure.storage.cleaned_repository import CleanedDataRepository

def list_all():
    """List all cleaned tariff codes"""
    repo = CleanedDataRepository()
    codes = repo.list_all()
    
    print("\n" + "=" * 70)
    print("All Cleaned Tariff Codes")
    print("=" * 70)
    print()
    
    if not codes:
        print("No cleaned data found")
        return
    
    for code in codes:
        data = repo.get(code)
        if data:
            print(f"  {code} - {data.product_description or 'N/A'}")
            print(f"    Cleaned: {data.cleaned_at}")
            print(f"    Sections: {len(data.sections)}")
            print()

def view_code(tariff_code: str):
    """View cleaned data for a tariff code"""
    repo = CleanedDataRepository()
    data = repo.get(tariff_code)
    
    print("\n" + "=" * 70)
    print(f"Cleaned Data for Tariff Code: {tariff_code}")
    print("=" * 70)
    print()
    
    if not data:
        print("No cleaned data found")
        return
    
    print(f"Tariff Code: {data.tariff_code}")
    print(f"Product: {data.product_description or 'N/A'}")
    print(f"Effective Date: {data.effective_date or 'N/A'}")
    print(f"Cleaned At: {data.cleaned_at}")
    print(f"Source Version: {data.source_version}")
    print()
    print(f"Sections ({len(data.sections)}):")
    print("-" * 70)
    
    for section_name, section in data.sections.items():
        print(f"\n{section_name}:")
        print(f"  Type: {section.section_type}")
        print(f"  Tables: {len(section.tables)}")
        
        for table in section.tables[:2]:  # Show first 2 tables
            print(f"    Table: {table.name}")
            print(f"      Headers: {', '.join(table.headers[:5])}")
            print(f"      Rows: {len(table.rows)}")
            if table.rows:
                # Show first row with cleaned values
                first_row = table.rows[0]
                print(f"      Sample row:")
                for header in table.headers[:3]:
                    value = first_row.row_data.get(header)
                    print(f"        {header}: {value}")

def export_code(tariff_code: str):
    """Export cleaned data to JSON"""
    repo = CleanedDataRepository()
    data = repo.get(tariff_code)
    
    if not data:
        print(f"No cleaned data found for {tariff_code}")
        return
    
    filename = f"cleaned_{tariff_code}_{data.cleaned_at.strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data.to_dict(), f, indent=2, ensure_ascii=False, default=str)
    
    print(f"Cleaned data exported to: {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        list_all()
    elif sys.argv[1] == "list":
        list_all()
    elif sys.argv[1] == "view" and len(sys.argv) > 2:
        view_code(sys.argv[2])
    elif sys.argv[1] == "export" and len(sys.argv) > 2:
        export_code(sys.argv[2])
    else:
        print("Usage:")
        print("  python view_cleaned_data.py list")
        print("  python view_cleaned_data.py view <tariff_code>")
        print("  python view_cleaned_data.py export <tariff_code>")


