import psycopg2
from psycopg2.extras import DictCursor
import json

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"

def print_result(title, cursor):
    print(f"\n{'='*80}")
    print(f" TABLE: {title}")
    print(f"{'='*80}")
    rows = cursor.fetchall()
    if not rows:
        print("No data found.")
        return
    
    # Get column names
    colnames = [desc[0] for desc in cursor.description]
    
    for row in rows:
        print("-" * 40)
        for col in colnames:
            val = row[col]
            if isinstance(val, (dict, list)):
                val = json.dumps(val, indent=2, ensure_ascii=False)
            elif hasattr(val, 'isoformat'):
                val = val.isoformat()
            print(f"{col:20}: {val}")

def main():
    try:
        conn = psycopg2.connect(DSN)
        with conn.cursor(cursor_factory=DictCursor) as cur:
            tables = ["sections", "chapters", "hs4_nodes", "hs6_nodes", "hs_products"]
            for table in tables:
                cur.execute(f"SELECT * FROM {table};")
                print_result(table, cur)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
