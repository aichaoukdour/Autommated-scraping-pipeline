import psycopg2

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"

def init_db():
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            print("Initializing database with init.sql...")
            with open("db/init.sql", "r", encoding="utf-8") as f:
                sql = f.read()
            cur.execute(sql)
            print("Database initialized successfully.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    init_db()
