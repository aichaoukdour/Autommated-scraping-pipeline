import os
import json

def load_json(data, path="data/output_gold.json"):
    """
    Save data to JSON file, creating the folder if it doesn't exist.
    """
    # 1. Ensure the folder exists
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # 2. Write the JSON file
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Successfully saved JSON to {path}")
