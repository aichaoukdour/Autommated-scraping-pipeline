import psycopg2

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"

def clean_db():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            print("Dropping existing tables...")
            cur.execute("DROP TABLE IF EXISTS rag_chunks CASCADE;")
            cur.execute("DROP TABLE IF EXISTS audit_logs CASCADE;")
            cur.execute("DROP TABLE IF EXISTS hs_products CASCADE;")
            cur.execute("DROP TABLE IF EXISTS hs6_nodes CASCADE;")
            cur.execute("DROP TABLE IF EXISTS hs4_nodes CASCADE;")
            cur.execute("DROP TABLE IF EXISTS chapters CASCADE;")
            cur.execute("DROP TABLE IF EXISTS sections CASCADE;")

            print("Database cleaned successfully.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clean_db()
