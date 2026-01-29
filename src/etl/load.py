import json
import time
from psycopg2.extras import Json
from typing import Optional
from scraper.config import logger

def record_audit_log(hs10: str, status: str, message: Optional[str], duration_ms: Optional[int], conn):
    """
    Record an entry in the audit_logs table.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit_logs (hs10, status, message, duration_ms)
            VALUES (%s, %s, %s, %s)
        """, (hs10, status, message, duration_ms))
    conn.commit()

from repository import HSRepository

def load_product(product: dict, conn):
    """
    Load one HS product using the Repository Pattern (DAL).
    """
    with conn.cursor() as cur:
        db = HSRepository(cur)
        section = product["hierarchy"]
        meta = section.get("meta", {"source": "ADII"})

        try:
            # 1. Sections & Chapters
            section_id = db.upsert_section(section["section_code"], section["section_label"], meta)
            chapter_id = db.upsert_chapter(section_id, section["chapter_code"], section["chapter_label"], meta)

            # 2. HS Nodes
            hs4_id = db.upsert_node("HS4", chapter_id, section["hs4"]["code"], section["hs4"].get("label"), meta)
            hs6_id = db.upsert_node("HS6", hs4_id, section["hs6"]["code"], section["hs6"].get("label"), meta)

            # 3. Core Product
            db.upsert_product(product, hs6_id)

        except Exception as e:
            conn.rollback()
            logger.debug(f"SQL Error for {product['hs_code']}: {e}")
            raise e

def load(products: list, dsn: str):
    """
    Entry point called from main ETL
    """
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        for product in products:
            load_product(product, conn)
        conn.commit()
        logger.info(f"Loaded {len(products)} HS products successfully")
    except Exception as e:
        conn.rollback()
        logger.error("LOAD FAILED - ROLLBACK")
        raise e
    finally:
        conn.close()
