"""
Value Objects - Immutable objects defined by their attributes
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
from enum import Enum


@dataclass(frozen=True)
class TariffCodeValue:
    """Value object for tariff code"""
    value: str
    
    def __post_init__(self):
        if not self.value:
            raise ValueError("Tariff code value cannot be empty")
        # Validate format
        clean_value = ''.join(c for c in self.value if c.isdigit())
        if len(clean_value) not in [8, 10]:
            raise ValueError(f"Invalid tariff code format: {self.value}")
    
    def __str__(self) -> str:
        return self.value


class ScrapingStrategy(str, Enum):
    """Strategies used for scraping"""
    URL_PATTERN = "URL pattern"
    INPUT_FIELD_PRESENCE = "Input field presence"
    FRAME_NAME_PATTERN = "Frame name pattern"
    FRAME_WITH_SELECTOR = "Frame with selector"
    CONTENT_SCORING = "Content scoring"
    EXACT_TEXT_MATCH = "Exact text match"
    TEXT_MATCH = "Text match"
    PARTIAL_TEXT_MATCH = "Partial text match"
    MANUAL_LINK_SEARCH = "Manual link search"


@dataclass(frozen=True)
class ScrapingConfiguration:
    """Configuration for scraping operations"""
    headless: bool = False
    max_retries: int = 3
    retry_delay: float = 2.0
    timeout: int = 30000
    monitor_changes: bool = True
    base_url: str = "https://www.douane.gov.ma/adil/"


@dataclass(frozen=True)
class MonitoringConfiguration:
    """Configuration for real-time monitoring"""
    interval_minutes: int = 60
    cache_ttl_seconds: int = 3600
    enable_change_detection: bool = True
    save_only_on_change: bool = False
    max_concurrent_scrapes: int = 1
    retry_failed_after_minutes: int = 15
