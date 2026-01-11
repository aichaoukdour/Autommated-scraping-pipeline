import psycopg2
from typing import Set

def get_existing_hs_codes(dsn: str) -> Set[str]:
    """Fetch all hs10 codes currently in the database."""
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute("SELECT hs10 FROM hs_products")
        codes = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return codes
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch existing codes from DB: {e}")
        return set()
