import json


def extract_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)[0]  # one HS code per file
