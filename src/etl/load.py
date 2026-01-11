# load.py

import json
import psycopg2
from psycopg2.extras import Json


import json
import psycopg2
from psycopg2.extras import Json

def load_product(product: dict, conn):
    """
    Load one HS product using the exact SQL patterns provided by the user.
    Hashes are generated at the DB level.
    """
    with conn.cursor() as cur:
        # 3.1 SECTION
        section = product["hierarchy"]
        cur.execute("""
            INSERT INTO sections (section_code, label, meta)
            VALUES (%s, %s, %s)
            ON CONFLICT (section_hash)
            DO UPDATE SET
              label = EXCLUDED.label,
              meta = EXCLUDED.meta,
              updated_at = now()
            RETURNING id
        """, (
            section["section_code"],
            section["section_label"],
            Json({"source": "ADII"})
        ))
        section_id = cur.fetchone()[0]

        # 3.2 CHAPTER
        cur.execute("""
            INSERT INTO chapters (section_id, chapter_code, label, meta)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chapter_hash)
            DO UPDATE SET
              label = EXCLUDED.label,
              meta = EXCLUDED.meta,
              updated_at = now()
            RETURNING id
        """, (
            section_id,
            section["chapter_code"],
            section["chapter_label"],
            Json({"source": "ADII"})
        ))
        chapter_id = cur.fetchone()[0]

        # 3.3 HS4
        hs4 = section["hs4"]
        cur.execute("""
            INSERT INTO hs4_nodes (chapter_id, hs4, label, present, meta)
            VALUES (%s, %s, %s, true, %s)
            ON CONFLICT (hs4_hash)
            DO UPDATE SET
              label = EXCLUDED.label,
              present = true,
              meta = EXCLUDED.meta,
              updated_at = now()
            RETURNING id
        """, (
            chapter_id,
            hs4["code"],
            hs4.get("label", "NA"),
            Json({"source": "ADII"})
        ))
        hs4_id = cur.fetchone()[0]

        # 3.4 HS6
        hs6 = section["hs6"]
        cur.execute("""
            INSERT INTO hs6_nodes (hs4_id, hs6, label, present, meta)
            VALUES (%s, %s, %s, true, %s)
            ON CONFLICT (hs6_hash)
            DO UPDATE SET
              label = EXCLUDED.label,
              present = true,
              meta = EXCLUDED.meta,
              updated_at = now()
            RETURNING id
        """, (
            hs4_id,
            hs6["code"],
            hs6.get("label", "NA"),
            Json({"source": "ADII"})
        ))
        hs6_id = cur.fetchone()[0]

        # 3.5 HS PRODUCT (CORE)
        cur.execute("""
            INSERT INTO hs_products (
                hs10,
                hs6_id,
                designation,
                unit_of_measure,
                entry_into_force_date,
                taxation,
                documents,
                agreements,
                import_duty_history,
                lineage,
                raw,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (hs10)
            DO UPDATE SET
                designation = EXCLUDED.designation,
                unit_of_measure = EXCLUDED.unit_of_measure,
                entry_into_force_date = EXCLUDED.entry_into_force_date,
                taxation = EXCLUDED.taxation,
                documents = EXCLUDED.documents,
                agreements = EXCLUDED.agreements,
                import_duty_history = EXCLUDED.import_duty_history,
                lineage = EXCLUDED.lineage,
                raw = EXCLUDED.raw,
                updated_at = now()
        """, (
            product["hs10"],
            hs6_id,
            product["designation"],
            product["unit_of_measure"],
            product["entry_into_force_date"],
            Json(product["taxation"]),
            Json(product["documents"]),
            Json(product["agreements"]),
            Json(product["import_duty_history"]),
            Json(product["lineage"]),
            Json(product["raw"])
        ))



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
        print(f"✅ Loaded {len(products)} HS products successfully")

    except Exception as e:
        conn.rollback()
        print("❌ LOAD FAILED — ROLLBACK")
        raise e

    finally:
        conn.close()
