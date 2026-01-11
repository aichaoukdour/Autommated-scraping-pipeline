import psycopg2
import csv
import os
from psycopg2.extras import DictCursor

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
# Resolve output path relative to script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output_csv")

def export_table_to_csv(table_name, conn):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        if not rows:
            print(f"Skipping {table_name}: No data found.")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        
        colnames = [desc[0] for desc in cur.description]
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=colnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
        
        print(f"Exported {table_name} to {filename}")

def main():
    try:
        conn = psycopg2.connect(DSN)
        tables = ["sections", "chapters", "hs4_nodes", "hs6_nodes", "hs_products"]
        for table in tables:
            export_table_to_csv(table, conn)
        conn.close()
    except Exception as e:
        print(f"Error during CSV export: {e}")

if __name__ == "__main__":
    main()
