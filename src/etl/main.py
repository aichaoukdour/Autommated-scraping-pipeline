# main.py

from extract import extract_json
from transform import transform
from load import load

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
INPUT_PATH = "../adil_detailed.json"


def process_data(raw_list: list, dsn: str = DSN):
    """Transform and load raw data payloads into the database."""
    products = []
    for raw in raw_list:
        try:
            product = transform(raw)
            products.append(product)
        except Exception as e:
            hs_code = raw.get("hs_code", "Unknown")
            print(f"❌ Failed to transform {hs_code}: {e}")

    if products:
        load(products, dsn)
    else:
        print("⚠️ No products were successfully transformed. Nothing to load.")

def run(input_path: str = INPUT_PATH):
    # Load all raw records from file
    raw_list = extract_json(input_path)
    print(f"🚀 Processing {len(raw_list)} record(s) from file.")
    process_data(raw_list)


if __name__ == "__main__":
    run()
