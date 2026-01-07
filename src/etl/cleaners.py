from ftfy import fix_text
from cleantext import clean
import unicodedata
import dateparser
from babel.numbers import parse_decimal
from typing import Optional


def clean_text_block(text: Optional[str]) -> Optional[str]:
    """
    Normalize and clean scraped French administrative text.
    Safe for ML, RAG, and analytics.
    """
    if not text:
        return None

    # Fix encoding issues (mojibake, broken accents)
    text = fix_text(text)

    # Normalize unicode (important for legal / HS texts)
    text = unicodedata.normalize("NFKC", text)

    # High-level text cleaning (NO aggressive removal)
    text = clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        strip_lines=True,
        no_line_breaks=False,
        lower=False
    )

    # Normalize excessive whitespace
    text = " ".join(text.split())

    return text.strip() or None


def parse_french_date(text: Optional[str]) -> Optional[str]:
    """
    Parse French dates safely into ISO format.
    """
    if not text:
        return None

    try:
        dt = dateparser.parse(text, languages=["fr"])
        return dt.date().isoformat() if dt else None
    except Exception:
        return None


def parse_percentage(value: Optional[str]) -> Optional[float]:
    """
    Parse French-formatted percentages safely.
    Examples:
      '2,5 %' -> 2.5
      '0,25%' -> 0.25
    """
    if not value:
        return None

    try:
        value = value.replace("%", "").strip()
        return float(parse_decimal(value, locale="fr_FR"))
    except Exception:
        return None
