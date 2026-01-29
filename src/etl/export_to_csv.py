import psycopg2
import csv
import os
import json
import re
from psycopg2.extras import DictCursor

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
# Resolve output path relative to script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output_csv")

# Columns that need RAG-optimized cleaning (remove hierarchical dash markers)
RAG_CLEAN_COLUMNS = ['hs6_label', 'designation', 'section_label', 'chapter_label', 'hs4_label', 'hs8_label']

def clean_hs_label_for_rag(text):
    """
    Cleans HS code hierarchical labels for RAG use.
    Removes leading dash markers (- -, - - -, etc.) and cleans semicolons.
    """
    if not text or text == 'NA':
        return text
    
    clean_text = text
    
    # Remove leading hierarchical dash patterns like "- -", "- - -", "– – –" etc.
    clean_text = re.sub(r'^[\s\-–—]+', '', clean_text)
    
    # Remove hierarchical markers after semicolons (e.g., ";- - - text" -> "; text")
    clean_text = re.sub(r';[\s\-–—]+', '; ', clean_text)
    
    # Clean up percentage markers like "%–" or "%" at weird positions
    clean_text = re.sub(r'%[\s\-–—]+', '', clean_text)
    
    # Remove standalone dash-space patterns in the middle of text
    clean_text = re.sub(r'\s[\-–—]\s[\-–—]\s[\-–—]\s', ' ', clean_text)
    clean_text = re.sub(r'\s[\-–—]\s[\-–—]\s', ' ', clean_text)
    
    # Clean up multiple spaces
    clean_text = ' '.join(clean_text.split())
    
    return clean_text.strip() or text


def export_table_to_csv(table_name, conn, filename=None):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()

        if not rows:
            print(f"Skipping {table_name}: No data found.")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        if filename is None:
            filename = os.path.join(OUTPUT_DIR, f"{table_name}.csv")

        colnames = [desc[0] for desc in cur.description]

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=colnames,
                delimiter=';'   # 👈 column separator
            )
            writer.writeheader()
            for row in rows:
                # Convert row to dict and serialize JSON objects
                row_dict = dict(row)
                for key, value in list(row_dict.items()):
                    if isinstance(value, (dict, list)):
                        row_dict[key] = json.dumps(value, ensure_ascii=False)
                    # Apply RAG cleaning to label columns
                    elif key in RAG_CLEAN_COLUMNS and isinstance(value, str):
                        row_dict[key] = clean_hs_label_for_rag(value)
                
                writer.writerow(row_dict)

        print(f"Exported {table_name} to {filename}")


def main():
    try:
        conn = psycopg2.connect(DSN)
        tables = ["sections", "chapters", "hs4_nodes", "hs6_nodes"]
        for table in tables:
            export_table_to_csv(table, conn)
        # Export hs_products to a new file to avoid permission issues
        export_table_to_csv("hs_products", conn, os.path.join(OUTPUT_DIR, "hs_products_v3.csv"))
        conn.close()
    except Exception as e:
        print(f"Error during CSV export: {e}")

if __name__ == "__main__":
    main()
