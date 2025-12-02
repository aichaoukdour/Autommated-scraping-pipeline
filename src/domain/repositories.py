"""
Repository Interfaces - Domain layer defines interfaces, infrastructure implements them
"""

from abc import ABC, abstractmethod
from typing import Optional, List
from .entities import TariffCode, ScrapedData


class ScrapingRepository(ABC):
    """Interface for scraping operations"""
    
    @abstractmethod
    def scrape(self, tariff_code: TariffCode) -> Optional[ScrapedData]:
        """Scrape data for a tariff code"""
        pass
    
    @abstractmethod
    def scrape_multiple(self, tariff_codes: List[TariffCode]) -> List[ScrapedData]:
        """Scrape multiple tariff codes"""
        pass


class FileRepository(ABC):
    """Interface for file operations"""
    
    @abstractmethod
    def save(self, data: ScrapedData, filepath: str) -> bool:
        """Save scraped data to file"""
        pass
    
    @abstractmethod
    def load(self, filepath: str) -> Optional[ScrapedData]:
        """Load scraped data from file"""
        pass
    
    @abstractmethod
    def load_tariff_codes(self, filepath: str) -> List[str]:
        """Load tariff codes from a file"""
        pass
    
    @abstractmethod
    def save_all(self, data_list: List[ScrapedData], filepath: str) -> bool:
        """Save multiple scraped data to a single file"""
        pass


class ChangeDetectionRepository(ABC):
    """Interface for change detection operations"""
    
    @abstractmethod
    def save_strategy_log(self, strategy_log: dict) -> bool:
        """Save strategy log for change detection"""
        pass
    
    @abstractmethod
    def load_strategy_log(self) -> Optional[dict]:
        """Load previous strategy log"""
        pass
    
    @abstractmethod
    def detect_changes(self, current_log: dict, previous_log: dict) -> List[str]:
        """Detect changes between current and previous strategy logs"""
        pass

