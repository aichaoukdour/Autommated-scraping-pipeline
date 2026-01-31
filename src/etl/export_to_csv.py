import csv
import json
import os
import psycopg2
from psycopg2.extras import DictCursor

# Ensure project root and src are in path for imports
import sys
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_src_dir = os.path.join(_project_root, "src")
for _path in [_project_root, _src_dir]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

from cleaners import clean_hs_label_for_rag
from scraper.config import ScraperConfig

# Configuration
config = ScraperConfig()
DSN = config.db_dsn
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_csv")
RAG_CLEAN_COLUMNS = ['hs6_label', 'designation', 'section_label', 'chapter_label', 'hs4_label', 'hs8_label']

def export_table(table_name, conn, filename=None):
    print(f"Exporting {table_name}...")
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        
        if not rows:
            print("No data found.")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        target_file = filename or os.path.join(OUTPUT_DIR, f"{table_name}.csv")

        with open(target_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[desc[0] for desc in cur.description], delimiter=';')
            writer.writeheader()
            
            for row in rows:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    # Handle JSON objects
                    if isinstance(value, (dict, list)):
                        row_dict[key] = json.dumps(value, ensure_ascii=False)
                    # Handle Text Cleaning
                    elif key in RAG_CLEAN_COLUMNS and isinstance(value, str):
                        row_dict[key] = clean_hs_label_for_rag(value)
                
                writer.writerow(row_dict)
    
    print(f"Done: {target_file}")

def main():
    try:
        conn = psycopg2.connect(DSN)
        
        # Export standard tables
        for table in ["sections", "chapters", "hs4_nodes", "hs6_nodes"]:
            export_table(table, conn)
            
        # Export main product table
        export_table("hs_products", conn, os.path.join(OUTPUT_DIR, "hs_products_v3.csv"))
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
