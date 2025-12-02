"""
Domain Layer - Core Business Logic
Contains entities, value objects, and domain interfaces
"""

from .entities import TariffCode, ScrapedData, SectionData, BasicInfo, Metadata
from .value_objects import TariffCodeValue, ScrapingStrategy
from .repositories import ScrapingRepository, FileRepository

__all__ = [
    'TariffCode',
    'ScrapedData',
    'SectionData',
    'BasicInfo',
    'Metadata',
    'TariffCodeValue',
    'ScrapingStrategy',
    'ScrapingRepository',
    'FileRepository',
]

