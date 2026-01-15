import time
import psycopg2
from extract import extract_json
from transform import transform
from load import load_product, record_audit_log

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
INPUT_PATH = "../adil_detailed.json"


def process_data(raw_list: list, dsn: str = DSN):
    """Transform and load raw data payloads into the database."""
    conn = psycopg2.connect(dsn)
    try:
        for raw in raw_list:
            hs_code = raw.get("hs_code", "Unknown")
            start_time = time.time()
            
            try:
                # 1. Transform
                product = transform(raw)
                
                # 2. Load
                load_product(product, conn)
                conn.commit()
                
                duration = int((time.time() - start_time) * 1000)
                record_audit_log(hs_code, "SUCCESS", None, duration, conn)
                
            except Exception as e:
                duration = int((time.time() - start_time) * 1000)
                error_msg = str(e)
                status = "FAILED"
                if "validation" in error_msg.lower() or "valueerror" in error_msg.lower():
                    status = "VALIDATION_ERROR"
                
                print(f"ERROR: {status} for {hs_code}: {error_msg}")
                record_audit_log(hs_code, status, error_msg, duration, conn)
                conn.rollback()

    finally:
        conn.close()

def run(input_path: str = INPUT_PATH):
    # Load all raw records from file
    raw_list = extract_json(input_path)
    print(f"Processing {len(raw_list)} record(s) from file.")
    process_data(raw_list)


if __name__ == "__main__":
    run()
