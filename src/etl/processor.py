import time
import psycopg2
from extract import extract_json
from transform import transform
from load import load_product, record_audit_log
from scraper.config import logger, ScraperConfig

_config = ScraperConfig()
DSN = _config.db_dsn

def process_single_record(raw: dict, conn, commit_on_success: bool = False):
    """Transform and load a single raw record into the database."""
    hs_code = raw.get("hs_code", "Unknown")
    start_time = time.time()
    
    try:
        # 1. Transform
        product = transform(raw)
        
        # 2. Load
        load_product(product, conn)
        
        if commit_on_success:
            conn.commit()
            
        duration = int((time.time() - start_time) * 1000)
        record_audit_log(hs_code, "SUCCESS", None, duration, conn)
        logger.info(f"Success: {hs_code}")
        
    except Exception as e:
        duration = int((time.time() - start_time) * 1000)
        error_msg = str(e)
        status = "FAILED"
        if "validation" in error_msg.lower() or "valueerror" in error_msg.lower():
            status = "VALIDATION_ERROR"
        
        logger.error(f"{status} for {hs_code}: {error_msg}")
        record_audit_log(hs_code, status, error_msg, duration, conn)
        conn.rollback()

def process_data(raw_list: list, dsn: str = DSN):
    """Process a batch of raw records (backward compatibility)."""
    conn = psycopg2.connect(dsn)
    try:
        for raw in raw_list:
            process_single_record(raw, conn)
    finally:
        conn.close()
