# main.py

from extract import extract_json
from transform import transform
from load import load

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"
INPUT_PATH = "../adil_detailed.json"


def run():
    raw = extract_json(INPUT_PATH)
    product = transform(raw)

    load([product], DSN)


if __name__ == "__main__":
    run()
