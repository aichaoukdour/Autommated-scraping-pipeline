import json


def extract_json(path: str) -> list:
    """Extract a list of raw product payloads from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
