"""
Data Transfer Objects (DTOs) - Data structures for application layer
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class ScrapeTariffCodeRequest:
    """Request DTO for scraping a single tariff code"""
    tariff_code: str
    headless: bool = False
    monitor_changes: bool = True


@dataclass
class ScrapingResult:
    """Result of a scraping operation"""
    success: bool
    tariff_code: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


@dataclass
class ScrapeTariffCodeResponse:
    """Response DTO for scraping operation"""
    result: ScrapingResult
    strategy_used: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


@dataclass
class ScrapeMultipleTariffCodesRequest:
    """Request DTO for scraping multiple tariff codes"""
    tariff_codes: List[str]
    headless: bool = False
    monitor_changes: bool = True
    delay_between_requests: float = 5.0


@dataclass
class ScrapeMultipleTariffCodesResponse:
    """Response DTO for multiple scraping operations"""
    results: List[ScrapingResult]
    total_count: int
    success_count: int
    failed_count: int
    changes_detected: List[str] = None
    
    def __post_init__(self):
        if self.changes_detected is None:
            self.changes_detected = []


@dataclass
class ChangeDetectionReport:
    """Report of detected changes"""
    timestamp: datetime
    changes: List[str]
    previous_timestamp: Optional[datetime] = None
    strategy_changes: Optional[Dict[str, Any]] = None

