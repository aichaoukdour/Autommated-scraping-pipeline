import json
import psycopg2
from psycopg2.extras import Json
from typing import Optional

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

def load_product(product: dict, conn):
    """
    Load one HS product using the exact SQL patterns provided by the user.
    Hashes are generated at the DB level.
    """
    with conn.cursor() as cur:
        section = product["hierarchy"]
        
        # Ensure 2-digit format for codes (critical for DB constraints)
        s_code = section["section_code"].zfill(2)
        c_code = section["chapter_code"].zfill(2)

        try:
            # 3.1 SECTION - Find or create
            cur.execute("SELECT id FROM sections WHERE section_code = %s LIMIT 1", (s_code,))
            row = cur.fetchone()
            if row:
                section_id = row[0]
            else:
                cur.execute("""
                    INSERT INTO sections (section_code, label, meta)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (
                    s_code,
                    section["section_label"],
                    Json(section.get("meta", {"source": "ADII"}))
                ))
                section_id = cur.fetchone()[0]

            # 3.2 CHAPTER - Find or create
            cur.execute("SELECT id FROM chapters WHERE section_id = %s AND chapter_code = %s LIMIT 1", (section_id, c_code))
            row = cur.fetchone()
            if row:
                chapter_id = row[0]
            else:
                cur.execute("""
                    INSERT INTO chapters (section_id, chapter_code, label, meta)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (
                    section_id,
                    c_code,
                    section["chapter_label"],
                    Json(section.get("meta", {"source": "ADII"}))
                ))
                chapter_id = cur.fetchone()[0]

            # 3.3 HS4 - Find or create
            hs4 = section["hs4"]
            cur.execute("SELECT id FROM hs4_nodes WHERE chapter_id = %s AND hs4 = %s LIMIT 1", (chapter_id, hs4["code"]))
            row = cur.fetchone()
            if row:
                hs4_id = row[0]
                # Update presence if needed
                cur.execute("UPDATE hs4_nodes SET present = %s WHERE id = %s", (hs4.get("label") is not None, hs4_id))
            else:
                cur.execute("""
                    INSERT INTO hs4_nodes (chapter_id, hs4, label, present, meta)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    chapter_id,
                    hs4["code"],
                    hs4.get("label"),
                    hs4.get("label") is not None,
                    Json(section.get("meta", {"source": "ADII"}))
                ))
                hs4_id = cur.fetchone()[0]
 
            # 3.4 HS6 - Find or create
            hs6 = section["hs6"]
            cur.execute("SELECT id FROM hs6_nodes WHERE hs4_id = %s AND hs6 = %s LIMIT 1", (hs4_id, hs6["code"]))
            row = cur.fetchone()
            if row:
                hs6_id = row[0]
                # Update presence if needed
                cur.execute("UPDATE hs6_nodes SET present = %s WHERE id = %s", (hs6.get("label") is not None, hs6_id))
            else:
                cur.execute("""
                    INSERT INTO hs6_nodes (hs4_id, hs6, label, present, meta)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    hs4_id,
                    hs6["code"],
                    hs6.get("label"),
                    hs6.get("label") is not None,
                    Json(section.get("meta", {"source": "ADII"}))
                ))
                hs6_id = cur.fetchone()[0]
 
            # 3.5 HS PRODUCT (CORE)
            cur.execute("""
                INSERT INTO hs_products (
                    hs10,
                    hs6_id,
                    hs8_label,
                    section_label,
                    chapter_label,
                    hs4_label,
                    hs6_label,
                    designation,
                    unit_of_measure,
                    taxation,
                    documents,
                    agreements,
                    import_duty_history,
                    lineage,
                    raw,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (hs10)
                DO UPDATE SET
                    hs8_label = EXCLUDED.hs8_label,
                    section_label = EXCLUDED.section_label,
                    chapter_label = EXCLUDED.chapter_label,
                    hs4_label = EXCLUDED.hs4_label,
                    hs6_label = EXCLUDED.hs6_label,
                    designation = EXCLUDED.designation,
                    unit_of_measure = EXCLUDED.unit_of_measure,
                    taxation = EXCLUDED.taxation,
                    documents = EXCLUDED.documents,
                    agreements = EXCLUDED.agreements,
                    import_duty_history = EXCLUDED.import_duty_history,
                    lineage = EXCLUDED.lineage,
                    raw = EXCLUDED.raw,
                    updated_at = now()
            """, (
                product["hs_code"],
                hs6_id,
                product.get("hs8_label"),
                product.get("section_label"),
                product.get("chapter_label"),
                product.get("hs4_label"),
                product.get("hs6_label"),
                product["designation"],
                product["unit_of_measure"],
                Json(product["taxation"]),
                Json(product["documents"]),
                Json(product["accord_convention"]),
                Json(product["historique"]),
                Json(product["lineage"]),
                Json(product["raw"])
            ))

        except Exception as e:
            conn.rollback()
            err_details = f"{e} | Codes: S={s_code}, C={c_code}"
            print(f"DEBUG SQL Error for {product['hs_code']}: {err_details}")
            raise Exception(err_details) from e

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
        print(f"Loaded {len(products)} HS products successfully")
    except Exception as e:
        conn.rollback()
        print("LOAD FAILED - ROLLBACK")
        raise e
    finally:
        conn.close()
