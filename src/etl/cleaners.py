from typing import Optional, Callable, Iterable
import re
from ftfy import fix_text
from cleantext import clean
from babel.numbers import parse_decimal
from cleaning_constants import BOILERPLATE, HS_PATTERNS
from dateparser import parse as dateparse


# Core Definitions
def normalize_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = fix_text(text)

    text = clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        lower=False,
        no_line_breaks=False
    )
    return " ".join(text.split()) if text else None


class RegexCleaner:
    def __init__(self, rules: Iterable[tuple[str, str]], flags: int = 0):
        self._patterns = [(re.compile(p, flags), r) for p, r in rules]

    def __call__(self, text: str) -> str:
        for p, r in self._patterns:
            text = p.sub(r, text)
        return text


# Create cleaner instances (after class is defined)
_HS_CLEANER = RegexCleaner(HS_PATTERNS)
_ADIL_CLEANER = RegexCleaner([
    ("|".join(map(re.escape, BOILERPLATE)), ""),
    (r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", ""),
    (r"[*-]{3,}", ""),
], flags=re.IGNORECASE)



def _pipeline(text: str, *steps: Callable[[str], str]) -> Optional[str]:
    if not text:
        return None
    for step in steps:
        text = step(text)
    return normalize_text(text)


# Domain Logic (HS, ADIL)
def clean_hs_label_for_rag(text: Optional[str]) -> Optional[str]:
    return _pipeline(text, _HS_CLEANER) if text else None


def remove_adil_boilerplate(text: Optional[str]) -> Optional[str]:
    return _pipeline(text, _ADIL_CLEANER) if text else None


# Extractors
def parse_percentage(value: Optional[str]) -> Optional[float]:
    if not value: return None
    try: return float(parse_decimal(value.replace("%", "").strip(), locale="fr_FR"))
    except: return None

def parse_french_date(text: Optional[str]) -> Optional[str]:
    if not text: return None
    parsed = dateparse(text, languages=["fr"])
    return parsed.date().isoformat() if parsed else None

