"""
Moroccan Customs ADIL Scraper - Refactored
Production-ready web scraper with clean architecture
"""

import json
import csv
import time
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException
)


# Configuration
@dataclass
class ScraperConfig:
    """Scraper configuration settings"""
    base_url: str = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    max_retries: int = 3
    wait_timeout: int = 15  # Increased from 10
    page_load_delay: int = 4  # Increased from 3
    section_load_delay: int = 3  # Increased from 2
    max_workers: int = 3
    headless: bool = True


# Data Models
@dataclass
class ContentData:
    """Structured content data"""
    raw_text: str
    metadata: Dict[str, str]
    key_values: Dict[str, str]
    tables: List[str]
    length: int


@dataclass
class SectionData:
    """Section scraping result"""
    section_name: str
    section_type: str
    content: ContentData
    scraped_at: str
    order: int
    status: Optional[str] = None


@dataclass
class ScrapeResult:
    """Complete scraping result for an HS code"""
    hs_code: str
    scraped_at: str
    scrape_status: str
    main_content: Dict[str, Any]
    sections: List[Dict[str, Any]]
    summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# Logging Setup
def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure logging with consistent format"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# Data Processing
class TextProcessor:
    """Extracts structured data from raw text"""
    
    KEY_VALUE_PATTERN = re.compile(r'([^:]+?)\s*(?:\([^)]*\))?\s*:\s*([^\n]+)')
    YEAR_PATTERN = re.compile(r'\b(19|20)\d{2}\b')
    
    METADATA_PATTERNS = {
        'position': r'Position tarifaire\s*:?\s*([^\n]+)',
        'source': r'Source\s*:?\s*([^\n]+)',
        'date': r'Situation du\s*:?\s*([^\n]+)',
        'period': r'Période.*?:?\s*([^\n]+)',
        'intercom': r'Intercom\s*:?\s*([^\n]+)',
        'unit': r'Unité.*?:?\s*([^\n]+)'
    }
    
    SECTION_TYPE_KEYWORDS = {
        'statistics': [r'\d{4}.*\d{4}', r'\d+[,\s]\d+'],
        'financial': [r'\d+\.?\d*\s*%'],
        'geography': ['pays', 'country', 'france', 'espagne', 'allemagne'],
        'classification': [r'section|chapitre|branche|division'],
        'regulatory': ['accord', 'treaty', 'restriction', 'prohibition', 'document']
    }
    
    @classmethod
    def extract_key_value_pairs(cls, text: str) -> Dict[str, str]:
        """Extract key-value pairs from text"""
        pairs = {}
        for key, value in cls.KEY_VALUE_PATTERN.findall(text):
            clean_key = key.strip()
            clean_value = value.strip()
            if clean_key and clean_value:
                pairs[clean_key] = clean_value
        return pairs
    
    @classmethod
    def extract_tables(cls, text: str) -> List[str]:
        """Extract table-like structures from text"""
        tables = []
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        current_table = []
        
        for line in lines:
            if cls.YEAR_PATTERN.search(line):
                current_table.append(line)
            elif current_table and any(c.isdigit() for c in line):
                current_table.append(line)
            else:
                if len(current_table) > 1:
                    tables.append('\n'.join(current_table))
                current_table = []
        
        if len(current_table) > 1:
            tables.append('\n'.join(current_table))
        
        return tables
    
    @classmethod
    def detect_section_type(cls, section_name: str, content: str) -> str:
        """Detect section type based on content patterns"""
        content_lower = content.lower()
        scores = defaultdict(int)
        
        for section_type, patterns in cls.SECTION_TYPE_KEYWORDS.items():
            for pattern in patterns:
                if isinstance(pattern, str) and not pattern.startswith(r'\d'):
                    if pattern in content_lower:
                        scores[section_type] += 2
                else:
                    if re.search(pattern, content):
                        scores[section_type] += 3
        
        return max(scores.items(), key=lambda x: x[1])[0] if scores else 'general'
    
    @classmethod
    def extract_metadata(cls, text: str) -> Dict[str, str]:
        """Extract metadata fields from text"""
        metadata = {}
        for key, pattern in cls.METADATA_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1).strip()
        return metadata
    
    @classmethod
    def process_content(cls, text: str) -> ContentData:
        """Process raw text into structured content"""
        return ContentData(
            raw_text=text,
            metadata=cls.extract_metadata(text),
            key_values=cls.extract_key_value_pairs(text),
            tables=cls.extract_tables(text),
            length=len(text)
        )


# Web Driver Management
class WebDriverManager:
    """Manages Chrome WebDriver lifecycle"""
    
    @staticmethod
    def create_driver(config: ScraperConfig) -> webdriver.Chrome:
        """Create configured Chrome WebDriver"""
        options = webdriver.ChromeOptions()
        
        if config.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=options)
        
        # Hide webdriver detection
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
        })
        
        return driver


# Core Scraper
class ADILScraper:
    """Scrapes HS code data from Moroccan customs website"""
    
    SKIP_KEYWORDS = ['nouvelle recherche', 'recherche', 'retour', 'accueil', 'home']
    
    def __init__(self, config: ScraperConfig = None):
        self.config = config or ScraperConfig()
        self.driver = WebDriverManager.create_driver(self.config)
        self.wait = WebDriverWait(self.driver, self.config.wait_timeout)
        self.processor = TextProcessor()
    
    def scrape_hs_code(self, hs_code: str) -> ScrapeResult:
        """Scrape data for a single HS code"""
        logger.info(f"Scraping HS code: {hs_code}")
        
        try:
            self._submit_search(hs_code)
            
            result = ScrapeResult(
                hs_code=hs_code,
                scraped_at=datetime.now().isoformat(),
                scrape_status="success",
                main_content={},
                sections=[]
            )
            
            self._scrape_main_content(result)
            self._scrape_all_sections(result)
            self._add_summary_stats(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error scraping {hs_code}: {e}")
            return ScrapeResult(
                hs_code=hs_code,
                scraped_at=datetime.now().isoformat(),
                scrape_status="error",
                main_content={},
                sections=[],
                error=str(e)
            )
    
    def _submit_search(self, hs_code: str) -> None:
        """Submit search form with HS code"""
        self.driver.get(self.config.base_url)
        time.sleep(self.config.page_load_delay)
        
        input_field = self.driver.find_element(By.NAME, "lposition")
        input_field.send_keys(hs_code)
        self.driver.find_element(By.NAME, "submit").click()
        
        time.sleep(self.config.page_load_delay + 1)
        logger.debug(f"Form submitted for {hs_code}")
    
    def _scrape_main_content(self, result: ScrapeResult) -> None:
        """Scrape main content frame"""
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2)
            
            body = self.driver.find_element(By.TAG_NAME, "body")
            main_text = body.text.strip()
            
            content = self.processor.process_content(main_text)
            result.main_content = asdict(content)
            
            logger.debug(f"Main content processed: {len(main_text)} chars")
            
        except Exception as e:
            logger.warning(f"Failed to scrape main content: {e}")
            result.scrape_status = "partial"
    
    def _scrape_all_sections(self, result: ScrapeResult) -> None:
        """Scrape all available sections"""
        try:
            section_links = self._get_section_links()
            logger.info(f"Found {len(section_links)} sections")
            
            for idx, link_info in enumerate(section_links):
                self._scrape_section_with_retry(result, link_info, idx, len(section_links))
                
        except Exception as e:
            logger.error(f"Failed to scrape sections: {e}")
            result.scrape_status = "partial"
    
    def _get_section_links(self) -> List[Dict[str, str]]:
        """Extract section links from sidebar"""
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(1)
        
        links = self.driver.find_elements(By.TAG_NAME, "a")
        section_links = []
        seen = set()
        
        for link in links:
            try:
                # Normalize whitespace to match XPath normalize-space() behavior
                raw_text = link.text or link.get_attribute("textContent")
                text = " ".join(raw_text.split())
                href = link.get_attribute("href")
                
                if (text and text not in seen and 
                    not any(kw in text.lower() for kw in self.SKIP_KEYWORDS)):
                    section_links.append({"name": text, "href": href})
                    seen.add(text)
            except:
                continue
        
        return section_links
    
    def _scrape_section_with_retry(
        self, 
        result: ScrapeResult, 
        link_info: Dict[str, str], 
        idx: int, 
        total: int
    ) -> None:
        """Scrape a section with retry logic"""
        section_name = link_info["name"]
        
        for attempt in range(self.config.max_retries):
            try:
                self._scrape_section(result, section_name, idx, total)
                return
            except (NoSuchElementException, StaleElementReferenceException, TimeoutException) as e:
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"Retry {attempt + 1} for {section_name} due to {type(e).__name__}")
                    logger.debug(f"Exception details: {e}")
                    time.sleep(2)
                    self.driver.switch_to.default_content()
                else:
                    logger.warning(f"Failed to scrape {section_name} after {self.config.max_retries} attempts. Last error: {type(e).__name__}")
                    logger.exception(f"Final exception details for {section_name}:")
                    self._add_unavailable_section(result, section_name, idx)
            except Exception as e:
                logger.error(f"Error scraping {section_name}: {e}")
                logger.exception("Full traceback:")
                break
    
    def _scrape_section(
        self, 
        result: ScrapeResult, 
        section_name: str, 
        idx: int, 
        total: int
    ) -> None:
        """Scrape a single section"""
        # Navigate to section
        self.driver.switch_to.default_content()
        time.sleep(1.5)  # Increased wait time
        self.driver.switch_to.frame(1)
        
        # Find link by text iteration (more robust than XPath)
        links = self.wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
        target_link = None
        
        for link in links:
            # Normalize whitespace
            raw_text = link.get_attribute("textContent") or ""
            text = " ".join(raw_text.split())
            
            if text == section_name:
                target_link = link
                break
        
        if not target_link:
            raise NoSuchElementException(f"Link not found for section: {section_name}")
            
        self.driver.execute_script("arguments[0].scrollIntoView(true);", target_link)
        time.sleep(1)
        
        try:
            target_link.click()
        except:
            self.driver.execute_script("arguments[0].click();", target_link)
        
        time.sleep(self.config.section_load_delay + 1)  # Extra wait for content
        
        # Extract content with multiple attempts
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(2)
        
        # Wait for content to load - try multiple strategies
        content_text = self._extract_frame_content()
        is_empty = not content_text or len(content_text) < 10
        
        section_data = self._build_section_data(
            section_name, 
            "N/A" if is_empty else content_text, 
            idx
        )
        
        result.sections.append(section_data)
        logger.info(f"[{idx+1}/{total}] {section_name}: {section_data['section_type']}")
    
    def _build_xpath(self, section_name: str) -> str:
        """Build XPath for section link"""
        quote = '"' if "'" in section_name else "'"
        # Use . instead of text() to handle nested elements (e.g. <b>Text</b>)
        return f'//a[normalize-space(.)={quote}{section_name}{quote}]'
    
    def _extract_frame_content(self) -> str:
        """Extract content from frame with multiple fallback strategies"""
        strategies = [
            # Strategy 1: Wait for body and get text
            lambda: self._wait_and_get_body_text(),
            # Strategy 2: Wait longer and try again
            lambda: self._wait_longer_and_retry(),
            # Strategy 3: Get all visible text from page
            lambda: self._get_all_visible_text(),
        ]
        
        for strategy in strategies:
            try:
                content = strategy()
                if content and len(content) >= 10:
                    return content
            except Exception as e:
                logger.debug(f"Strategy failed: {e}")
                continue
        
        return ""
    
    def _wait_and_get_body_text(self) -> str:
        """Wait for body element and extract text"""
        body = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)  # Give content time to render
        return body.text.strip()
    
    def _wait_longer_and_retry(self) -> str:
        """Wait longer for dynamic content"""
        time.sleep(3)
        body = self.driver.find_element(By.TAG_NAME, "body")
        return body.text.strip()
    
    def _get_all_visible_text(self) -> str:
        """Get all visible text using JavaScript"""
        script = """
        return document.body.innerText || document.body.textContent || '';
        """
        content = self.driver.execute_script(script)
        return content.strip() if content else ""
    
    def _build_section_data(self, section_name: str, content: str, idx: int) -> Dict[str, Any]:
        """Build section data dictionary"""
        is_empty = content == "N/A"
        
        if is_empty:
            processed_content = ContentData(
                raw_text="N/A",
                metadata={},
                key_values={},
                tables=[],
                length=0
            )
        else:
            processed_content = self.processor.process_content(content)
        
        return {
            "section_name": section_name,
            "section_type": "empty" if is_empty else self.processor.detect_section_type(section_name, content),
            "content": asdict(processed_content),
            "scraped_at": datetime.now().isoformat(),
            "order": idx
        }
    
    def _add_unavailable_section(self, result: ScrapeResult, section_name: str, idx: int) -> None:
        """Add placeholder for unavailable section"""
        result.sections.append({
            "section_name": section_name,
            "section_type": "unavailable",
            "content": {
                "raw_text": "N/A",
                "metadata": {},
                "key_values": {},
                "tables": [],
                "length": 0
            },
            "scraped_at": datetime.now().isoformat(),
            "order": idx,
            "status": "not_available"
        })
        logger.warning(f"Section unavailable: {section_name}")
    
    def _add_summary_stats(self, result: ScrapeResult) -> None:
        """Add summary statistics to result"""
        section_types = defaultdict(int)
        for section in result.sections:
            if "section_type" in section:
                section_types[section["section_type"]] += 1
        
        result.summary = {
            "total_sections": len(result.sections),
            "successful_sections": sum(1 for s in result.sections if "error" not in s),
            "failed_sections": sum(1 for s in result.sections if "error" in s),
            "section_types": dict(section_types)
        }
        
        logger.info(f"Summary: {result.summary['total_sections']} sections, types: {dict(section_types)}")
    
    def close(self) -> None:
        """Close WebDriver"""
        self.driver.quit()


# Orchestration
@contextmanager
def scraper_context(config: ScraperConfig = None):
    """Context manager for scraper lifecycle"""
    scraper = ADILScraper(config)
    try:
        yield scraper
    finally:
        scraper.close()


def scrape_single_code(hs_code: str, config: ScraperConfig = None) -> Dict[str, Any]:
    """Scrape a single HS code"""
    with scraper_context(config) as scraper:
        result = scraper.scrape_hs_code(hs_code)
        return asdict(result)


def process_csv_batch(
    csv_path: Path, 
    config: ScraperConfig = None,
    limit: Optional[int] = None,
    skip_codes: Optional[Set[str]] = None
) -> List[Dict[str, Any]]:
    """Process multiple HS codes from CSV"""
    hs_codes = read_hs_codes(csv_path, limit)
    
    if skip_codes:
        original_count = len(hs_codes)
        hs_codes = [c for c in hs_codes if c not in skip_codes]
        skipped_count = original_count - len(hs_codes)
        if skipped_count > 0:
            logger.info(f"⏭️ Skipping {skipped_count} HS codes already in database.")

    logger.info(f"Processing {len(hs_codes)} HS codes with {config.max_workers} workers")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_to_code = {
            executor.submit(scrape_single_code, code, config): code 
            for code in hs_codes
        }
        
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed {code} ({len(results)}/{len(hs_codes)})")
            except Exception as e:
                logger.error(f"Failed {code}: {e}")
    
    return results


# I/O Operations
def read_hs_codes(csv_path: Path, limit: Optional[int] = None) -> List[str]:
    """Read HS codes from CSV file"""
    hs_codes = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            hs_codes.append(row['hs_code'].strip())
            if limit and len(hs_codes) >= limit:
                break
    
    logger.info(f"Loaded {len(hs_codes)} HS codes from {csv_path}")
    return hs_codes


def save_results(
    results: List[Dict[str, Any]], 
    output_dir: Path = Path("."),
    config: ScraperConfig = None
) -> None:
    """Save scraping results to JSON files"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save detailed results
    detailed_path = output_dir / "adil_detailed.json"
    with open(detailed_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Save summary
    summary = build_summary(results, config)
    summary_path = output_dir / "adil_summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Results saved to {output_dir}")


def build_summary(results: List[Dict[str, Any]], config: ScraperConfig) -> Dict[str, Any]:
    """Build summary statistics"""
    total_sections = sum(r.get("summary", {}).get("total_sections", 0) for r in results)
    section_types = defaultdict(int)
    
    for result in results:
        if "summary" in result and "section_types" in result["summary"]:
            for stype, count in result["summary"]["section_types"].items():
                section_types[stype] += count
    
    return {
        "pipeline_metadata": {
            "run_timestamp": datetime.now().isoformat(),
            "total_codes_processed": len(results),
            "max_workers": config.max_workers
        },
        "status_breakdown": {
            "success": sum(1 for r in results if r.get("scrape_status") == "success"),
            "partial": sum(1 for r in results if r.get("scrape_status") == "partial"),
            "error": sum(1 for r in results if r.get("scrape_status") == "error")
        },
        "section_analytics": {
            "total_sections_scraped": total_sections,
            "avg_sections_per_code": round(total_sections / len(results), 2) if results else 0,
            "section_type_distribution": dict(section_types)
        }
    }


# Entry Point
def main(csv_path: Optional[Path] = None, output_dir: Path = Path("."), skip_codes: Optional[Set[str]] = None, save_to_file: bool = True):
    """Main execution function"""
    # Enable DEBUG logging
    logger.setLevel(logging.INFO)
    
    config = ScraperConfig(
        max_retries=3,  # Restore retries
        max_workers=3,
        headless=True
    )
    
    if csv_path is None:
        csv_path = Path("../Code Sh Import - Feuil.csv")
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return []
    
    results = process_csv_batch(csv_path, config, limit=15, skip_codes=skip_codes)
    
    if save_to_file:
        save_results(results, output_dir, config)
    
    logger.info("Processing complete")
    return results


if __name__ == "__main__":
    main()