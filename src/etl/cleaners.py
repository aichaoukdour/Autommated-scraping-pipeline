from ftfy import fix_text
from cleantext import clean
import unicodedata
from babel.numbers import parse_decimal
from typing import Optional
from dateparser import parse as dateparse

def clean_text_block(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = fix_text(text)
    text = unicodedata.normalize("NFKC", text)
    text = clean(text, fix_unicode=True, to_ascii=False, strip_lines=True,
                 no_line_breaks=False, lower=False)
    return " ".join(text.split()).strip() or None

def parse_percentage(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        value = value.replace("%", "").strip()
        return float(parse_decimal(value, locale="fr_FR"))
    except Exception:
        return None

def parse_french_date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    dt = dateparse(text, languages=["fr"])
    return dt.date().isoformat() if dt else None
