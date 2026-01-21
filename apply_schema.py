import psycopg2

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
SQL_FILE = "db/init.sql"

def apply_schema():
    try:
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        
        print(f"Reading {SQL_FILE}...")
        with open(SQL_FILE, 'r', encoding='utf-8') as f:
            sql = f.read()
            
        print("Applying schema...")
        cur.execute(sql)
        conn.commit()
        print("✅ Schema applied successfully.")
        
    except Exception as e:
        print(f"❌ Error applying schema: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    apply_schema()
