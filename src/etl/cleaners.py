from ftfy import fix_text
from cleantext import clean
import unicodedata
import dateparser
from babel.numbers import parse_decimal


def clean_text_block(text: str) -> str:
    if not text:
        return None
    text = fix_text(text)
    text = unicodedata.normalize("NFKC", text)
    text = clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        strip_lines=True,
        no_line_breaks=False
    )
    return text.strip()


def parse_french_date(text: str):
    if not text:
        return None
    dt = dateparser.parse(text, languages=["fr"])
    return dt.date().isoformat() if dt else None


def parse_percentage(value: str):
    if not value:
        return None
    value = value.replace("%", "").strip()
    return float(parse_decimal(value, locale="fr_FR"))
