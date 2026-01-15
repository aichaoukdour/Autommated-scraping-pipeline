import psycopg2
import csv
import os
import json
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
            writer = csv.DictWriter(
                f,
                fieldnames=colnames,
                delimiter=';'   # 👈 column separator
            )
            writer.writeheader()
            for row in rows:
                # Convert row to dict and serialize JSON objects
                row_dict = dict(row)
                for key, value in list(row_dict.items()):
                    if isinstance(value, (dict, list)):
                        row_dict[key] = json.dumps(value, ensure_ascii=False)
                
                writer.writerow(row_dict)

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
