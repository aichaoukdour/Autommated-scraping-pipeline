import json
import logging
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

class HSRepository:
    """
    Data Access Layer (Repository Pattern) for ADIL HS Products.
    Encapsulates all SQL logic to keep the ETL flow clean.
    """
    def __init__(self, cursor):
        self.cur = cursor

    def upsert_section(self, code, label, meta):
        self.cur.execute("""
            INSERT INTO sections (section_code, label, meta)
            VALUES (%s, %s, %s)
            ON CONFLICT (section_code) DO UPDATE SET section_code = EXCLUDED.section_code
            RETURNING id
        """, (code.zfill(2), label, Json(meta)))
        return self.cur.fetchone()[0]

    def upsert_chapter(self, section_id, code, label, meta):
        self.cur.execute("""
            INSERT INTO chapters (section_id, chapter_code, label, meta)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (section_id, chapter_code) DO UPDATE SET chapter_code = EXCLUDED.chapter_code
            RETURNING id
        """, (section_id, code.zfill(2), label, Json(meta)))
        return self.cur.fetchone()[0]

    def upsert_node(self, node_type, parent_id, code, label, meta):
        """Generic upsert for HS4 and HS6 nodes."""
        table = "hs4_nodes" if node_type == "HS4" else "hs6_nodes"
        parent_col = "chapter_id" if node_type == "HS4" else "hs4_id"
        code_col = "hs4" if node_type == "HS4" else "hs6"
        
        sql = f"""
            INSERT INTO {table} ({parent_col}, {code_col}, label, present, meta)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ({parent_col}, {code_col}) DO UPDATE SET 
                label = EXCLUDED.label,
                present = EXCLUDED.present
            RETURNING id
        """
        self.cur.execute(sql, (
            parent_id,
            code,
            label,
            label is not None,
            Json(meta)
        ))
        return self.cur.fetchone()[0]

    def upsert_product(self, product, hs6_id):
        self.cur.execute("""
            INSERT INTO hs_products (
                hs10, hs6_id, hs8_label, section_label, chapter_label,
                hs4_label, hs6_label, designation, unit_of_measure,
                taxation, documents, agreements, import_duty_history,
                lineage, raw, canonical_hash, canonical_text, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
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
                canonical_hash = EXCLUDED.canonical_hash,
                canonical_text = EXCLUDED.canonical_text,
                updated_at = CASE 
                    WHEN hs_products.canonical_hash IS DISTINCT FROM EXCLUDED.canonical_hash THEN now() 
                    ELSE hs_products.updated_at 
                END
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
            Json(product["raw"]),
            product["canonical_hash"],
            product.get("canonical_text")
        ))
