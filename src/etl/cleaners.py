from typing import Optional, Callable, Iterable
import re
import unicodedata

from ftfy import fix_text
from cleantext import clean
from babel.numbers import parse_decimal
from dateparser import parse as dateparse


# ===================================================================
# Core utilities
# ===================================================================

CleanerFn = Callable[[str], str]


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split()).strip()


def safe_apply(text: Optional[str], steps: Iterable[CleanerFn]) -> Optional[str]:
    """Apply cleaning steps safely in sequence."""
    if not text:
        return None

    for step in steps:
        text = step(text)

    text = normalize_whitespace(text)
    return text or None


# ===================================================================
# Regex-based cleaner abstraction
# ===================================================================

class RegexCleaner:
    """Reusable regex substitution engine."""

    def __init__(self, rules: Iterable[tuple[str, str]], flags: int = 0):
        self._patterns = [
            (re.compile(pattern, flags), replacement)
            for pattern, replacement in rules
        ]

    def __call__(self, text: str) -> str:
        for pattern, replacement in self._patterns:
            text = pattern.sub(replacement, text)
        return text


# ===================================================================
# Generic text normalization
# ===================================================================

def unicode_normalizer(text: str) -> str:
    return unicodedata.normalize("NFKC", fix_text(text))


def cleantext_normalizer(text: str) -> str:
    return clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        strip_lines=True,
        no_line_breaks=False,
        lower=False,
    )


def clean_text_block(text: Optional[str]) -> Optional[str]:
    return safe_apply(
        text,
        steps=[
            unicode_normalizer,
            cleantext_normalizer,
        ],
    )


# ===================================================================
# Parsing helpers
# ===================================================================

def parse_percentage(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(parse_decimal(value.replace("%", "").strip(), locale="fr_FR"))
    except Exception:
        return None


def parse_french_date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    parsed = dateparse(text, languages=["fr"])
    return parsed.date().isoformat() if parsed else None


# ===================================================================
# HS label cleaner (RAG-optimized)
# ===================================================================

HS_LABEL_CLEANER = RegexCleaner(
    rules=[
        (r"^[\s\-–—]+", ""),
        (r";[\s\-–—]+", "; "),
        (r"%[\s\-–—]+", ""),
        (r"\s[\-–—]\s[\-–—]\s[\-–—]\s", " "),
        (r"\s[\-–—]\s[\-–—]\s", " "),
    ]
)


def clean_hs_label_for_rag(text: Optional[str]) -> Optional[str]:
    return safe_apply(text, steps=[HS_LABEL_CLEANER])


# ===================================================================
# ADiL boilerplate cleaner
# ===================================================================

BOILERPLATE_PHRASES = [
    "ADiL", "Nomenclature douanière.", "Position tarifaire :",
    "Source : ADII", "Office des Changes", "DESIGNATION DU PRODUIT :",
    "Droits et Taxes à l'Import.", "Documents et Normes à l'Import.",
    "Classification Nationale des Echanges Commerciaux",
    "Classification Internationale des Echanges Commerciaux",
    "Merci de patienter quelques instants...",
    "Vous êtes sur la page", "Page Suivante",
    "La douane marocaine vous donne des droits d’opposition, de rectification et de suppression.",
]

BOILERPLATE_REGEX = RegexCleaner(
    rules=[("|".join(map(re.escape, BOILERPLATE_PHRASES)), "")],
    flags=re.IGNORECASE,
)

NOISE_CLEANER = RegexCleaner(
    rules=[
        (r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", ""),
        (r"\*{2,}", ""),
        (r"-{3,}", ""),
    ]
)


def remove_adil_boilerplate(text: Optional[str]) -> Optional[str]:
    return safe_apply(
        text,
        steps=[
            BOILERPLATE_REGEX,
            NOISE_CLEANER,
            clean_text_block,
        ],
    )
