# main.py

from extract import extract_json
from transform import transform
from load import load

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
INPUT_PATH = "../adil_detailed.json"


def run():
    # Load all raw records
    raw_list = extract_json(INPUT_PATH)
    
    print(f"🚀 Processing {len(raw_list)} record(s).")

    products = []
    for raw in raw_list:
        try:
            product = transform(raw)
            products.append(product)
        except Exception as e:
            hs_code = raw.get("hs_code", "Unknown")
            print(f"❌ Failed to transform {hs_code}: {e}")

    if products:
        load(products, DSN)
    else:
        print("⚠️ No products were successfully transformed. Nothing to load.")


if __name__ == "__main__":
    run()
