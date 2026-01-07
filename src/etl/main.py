from extract import extract_json
from transform import transform
from load import load_json


INPUT_PATH = "../adil_detailed.json"
OUTPUT_PATH = "data/output_gold.json"


def run():
    raw = extract_json(INPUT_PATH)
    gold = transform(raw)
    load_json(gold, OUTPUT_PATH)
    print("ETL completed successfully.")


if __name__ == "__main__":
    run()
