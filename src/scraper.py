"""
Moroccan Customs ADIL Scraper - BeautifulSoup Enhanced
Production-ready web scraper with clean architecture and robust HTML parsing.
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

# --- BeautifulSoup Import ---
from bs4 import BeautifulSoup, Tag

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
    wait_timeout: int = 5
    page_load_delay: int = 3
    section_load_delay: int = 2
    max_workers: int = 3
    headless: bool = True

# Data Models
@dataclass
class ContentData:
    """Structured content data"""
    raw_text: str
    metadata: Dict[str, str]
    key_values: Dict[str, str]
    tables: List[Dict[str, Any]] 
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
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger("ADIL_Scraper")

logger = setup_logging()

# --- BeautifulSoup Logic ---
class TextProcessor:
    """Extracts structured data from HTML content using BeautifulSoup"""
    
    KEY_VALUE_PATTERN = re.compile(r'([^:]+?)\s*(?:\([^)]*\))?\s*:\s*([^\n]+)')
    
    METADATA_PATTERNS = {
        'position': r'Position tarifaire\s*:?\s*([^\n<]+)',
        'source': r'Source\s*:?\s*([^\n<]+)',
        'date': r'Situation du\s*:?\s*([^\n<]+)',
        'unit': r'Unité.*?:?\s*([^\n<]+)'
    }
    
    SECTION_TYPE_KEYWORDS = {
        'statistics': [r'\d{4}.*\d{4}', r'importation', 'exportation', 'statistique'],
        'financial': [r'\d+\.?\d*\s*%', 'droit', 'taxe', 'tva'],
        'geography': ['pays', 'country', 'ue', 'agadir', 'turquie'],
        'regulatory': ['accord', 'restriction', 'prohibition', 'document', 'norme']
    }

    @classmethod
    def process_content(cls, html_content: str) -> ContentData:
        """Process HTML into structured content using BeautifulSoup"""
        if not html_content:
            return cls._empty_content()

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements to clean up text extraction
        for script in soup(["script", "style"]):
            script.decompose()

        # Get clean text
        clean_text = soup.get_text(separator="\n", strip=True)
        
        return ContentData(
            raw_text=clean_text,
            metadata=cls.extract_metadata(clean_text),
            key_values=cls.extract_key_value_pairs(clean_text),
            tables=cls.extract_html_tables(soup),  # Pass soup object directly
            length=len(clean_text)
        )

    @staticmethod
    def _empty_content() -> ContentData:
        return ContentData("", {}, {}, [], 0)

    @classmethod
    def extract_html_tables(cls, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extracts data from actual HTML <table> tags.
        Filters out layout tables (tables used for visual structure only).
        """
        tables_data = []
        html_tables = soup.find_all("table")

        for table in html_tables:
            # Skip tables inside other tables (nested) to avoid duplication
            if table.find_parent("table"):
                continue

            parsed_table = cls._parse_html_table(table)
            if parsed_table:
                tables_data.append(parsed_table)
        
        return tables_data

    @classmethod
    def _parse_html_table(cls, table_tag: Tag) -> Optional[Dict[str, Any]]:
        """Parses a single BeautifulSoup table tag"""
        rows = table_tag.find_all("tr")
        if not rows or len(rows) < 2:
            return None

        # Strategy: Look for the best candidate for a header row
        # Usually the first row with <th> or the first row with multiple <td>
        header_row = None
        data_start_index = 0

        # Try to find a row with <th> tags
        for i, row in enumerate(rows[:3]): # Check first 3 rows
            if row.find("th"):
                header_row = row
                data_start_index = i + 1
                break
        
        # Fallback: First row with meaningful text if no <th> found
        if not header_row:
            header_row = rows[0]
            data_start_index = 1

        # Extract headers
        headers = [cls._clean_cell(cell.get_text()) for cell in header_row.find_all(["th", "td"])]
        
        # Filter out purely empty header lists
        if not any(h for h in headers if h):
            return None

        data_rows = []
        for row in rows[data_start_index:]:
            cells = row.find_all(["td", "th"])
            
            # Simple heuristic: ignore rows that don't match header count roughly
            # (Allows for slight mismatch due to colspans, but skips clearly wrong rows)
            if len(cells) == 0:
                continue

            row_data = {}
            has_data = False
            
            # Safe looping using zip (stops at shortest list)
            for idx, cell in enumerate(cells):
                if idx < len(headers) and headers[idx]:
                    val = cls._normalize_cell(cell.get_text())
                    row_data[headers[idx]] = val
                    if val:
                        has_data = True
            
            if has_data:
                data_rows.append(row_data)

        if not data_rows:
            return None

        return {
            "headers": [h for h in headers if h],
            "rows": data_rows,
            "row_count": len(data_rows)
        }

    @staticmethod
    def _clean_cell(text: str) -> str:
        """Clean header text"""
        # Replace non-breaking spaces and newlines
        text = text.replace('\xa0', ' ').replace('\n', ' ')
        return " ".join(text.strip().split())

    @staticmethod
    def _normalize_cell(value: str) -> Any:
        v = value.strip()
        if not v: return ""
        
        # Percentages
        if v.endswith("%"):
            try:
                return float(v.replace("%", "").replace(",", ".")) / 100
            except ValueError:
                return v
        
        return v

    @classmethod
    def extract_key_value_pairs(cls, text: str) -> Dict[str, str]:
        pairs = {}
        for key, value in cls.KEY_VALUE_PATTERN.findall(text):
            clean_key = key.strip()
            clean_value = value.strip()
            if clean_key and clean_value and len(clean_value) < 200: # limit length to avoid false positives
                pairs[clean_key] = clean_value
        return pairs
    
    @classmethod
    def extract_metadata(cls, text: str) -> Dict[str, str]:
        metadata = {}
        for key, pattern in cls.METADATA_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1).strip()
        return metadata

    @classmethod
    def detect_section_type(cls, section_name: str, content: str) -> str:
        clean_content = re.sub(r'<[^>]+>', ' ', content).lower()
        section_name_lower = section_name.lower()
        
        # Check section name first (stronger signal)
        for s_type, keywords in cls.SECTION_TYPE_KEYWORDS.items():
            if any(k in section_name_lower for k in keywords if isinstance(k, str) and not k.startswith('\\')):
                return s_type

        # Check content
        scores = defaultdict(int)
        for section_type, patterns in cls.SECTION_TYPE_KEYWORDS.items():
            for pattern in patterns:
                if re.search(pattern, clean_content):
                    scores[section_type] += 1
        
        if not scores:
            return 'general_info'
        return max(scores.items(), key=lambda x: x[1])[0]


# Web Driver Management
class WebDriverManager:
    @staticmethod
    def create_driver(config: ScraperConfig) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        if config.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--log-level=3') # Suppress selenium logs
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=options)
        return driver


# Core Scraper
class ADILScraper:
    SKIP_KEYWORDS = ['nouvelle recherche', 'recherche', 'retour', 'accueil', 'home']
    
    def __init__(self, config: ScraperConfig = None):
        self.config = config or ScraperConfig()
        self.driver = WebDriverManager.create_driver(self.config)
        self.wait = WebDriverWait(self.driver, self.config.wait_timeout)
        self.processor = TextProcessor()
    
    def scrape_hs_code(self, hs_code: str) -> ScrapeResult:
        logger.info(f"Processing HS Code: {hs_code}")
        
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
            
            return result
            
        except Exception as e:
            logger.error(f"Critical error on {hs_code}: {str(e)}")
            return ScrapeResult(
                hs_code=hs_code,
                scraped_at=datetime.now().isoformat(),
                scrape_status="error",
                main_content={},
                sections=[],
                error=str(e)
            )
    
    def _submit_search(self, hs_code: str) -> None:
        self.driver.get(self.config.base_url)
        # Handle potential alert boxes or popups here if they existed
        
        input_field = self.wait.until(EC.presence_of_element_located((By.NAME, "lposition")))
        input_field.clear()
        input_field.send_keys(hs_code)
        
        submit_btn = self.driver.find_element(By.NAME, "submit")
        submit_btn.click()
        
        # Wait for frame 2 to load which indicates success
        try:
            self.wait.until(EC.frame_to_be_available_and_switch_to_it(2))
            self.driver.switch_to.default_content() # Switch back
        except TimeoutException:
            raise Exception("Search yielded no results or timed out.")
    
    def _scrape_main_content(self, result: ScrapeResult) -> None:
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2)
            
            # Grab HTML for BeautifulSoup
            html_content = self.driver.find_element(By.TAG_NAME, "body").get_attribute("outerHTML")
            
            content = self.processor.process_content(html_content)
            result.main_content = asdict(content)
            
        except Exception as e:
            logger.warning(f"Main content scrape failed: {e}")
            result.scrape_status = "partial"

    def _scrape_all_sections(self, result: ScrapeResult) -> None:
        section_links = self._get_section_links()
        
        for idx, link_info in enumerate(section_links):
            # Pass retry attempt 0
            self._process_single_section(result, link_info, idx)

    def _get_section_links(self) -> List[Dict[str, str]]:
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(1) # Navigation frame
        
        links = self.driver.find_elements(By.TAG_NAME, "a")
        section_links = []
        seen = set()
        
        for link in links:
            txt = link.get_attribute("textContent").strip()
            # Clean spaces
            txt = " ".join(txt.split())
            
            if txt and txt not in seen and not any(k in txt.lower() for k in self.SKIP_KEYWORDS):
                section_links.append({"name": txt})
                seen.add(txt)
                
        return section_links

    def _process_single_section(self, result: ScrapeResult, link_info: Dict[str, str], idx: int):
        section_name = link_info["name"]
        
        try:
            # 1. Click Link
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(1)
            
            # Find link again (DOM refresh)
            links = self.driver.find_elements(By.TAG_NAME, "a")
            target = next((l for l in links if " ".join(l.text.split()) == section_name), None)
            
            if not target:
                logger.warning(f"Link lost: {section_name}")
                return

            self.driver.execute_script("arguments[0].click();", target)
            time.sleep(self.config.section_load_delay)
            
            # 2. Extract Content
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2) # Content frame
            
            body = self.driver.find_element(By.TAG_NAME, "body")
            html_content = body.get_attribute("outerHTML")
            
            # 3. Process with BeautifulSoup
            processed = self.processor.process_content(html_content)
            section_type = self.processor.detect_section_type(section_name, processed.raw_text)
            
            result.sections.append({
                "section_name": section_name,
                "section_type": section_type,
                "content": asdict(processed),
                "order": idx,
                "scraped_at": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.warning(f"Failed section {section_name}: {e}")

    def close(self):
        try:
            self.driver.quit()
        except:
            pass

# Batch Processing
def scrape_single_code(hs_code: str, config: ScraperConfig) -> Dict:
    scraper = ADILScraper(config)
    try:
        result = scraper.scrape_hs_code(hs_code)
        return asdict(result)
    finally:
        scraper.close()

def main(
    csv_path: Optional[Path] = None, 
    output_dir: Path = Path("."), 
    skip_codes: Optional[Set[str]] = None, 
    save_to_file: bool = True,
    limit: Optional[int] = None
):
    """Main execution function compatible with master pipeline"""
    # 1. Setup
    config = ScraperConfig(headless=True, max_workers=3)
    if csv_path is None:
        csv_path = Path("Code Sh Import - Feuil.csv")
    
    # 2. Read Codes
    codes = []
    if csv_path.exists():
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            codes = [row['hs_code'].strip() for row in reader if row.get('hs_code')]
    else:
        logger.error(f"CSV file not found: {csv_path}")
        return []

    # 3. Apply Filters
    if skip_codes:
        initial_count = len(codes)
        codes = [c for c in codes if c not in skip_codes]
        logger.info(f"Skipping {initial_count - len(codes)} already processed codes")

    if limit:
        codes = codes[:limit]
        logger.info(f"Limiting to first {limit} codes")

    if not codes:
        logger.info("No codes to process.")
        return []

    # 4. Run Batch
    logger.info(f"Starting batch process for {len(codes)} codes (Streaming Mode)...")
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_map = {executor.submit(scrape_single_code, code, config): code for code in codes}
        
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                res = future.result()
                logger.info(f"✅ Finished Scraping {code}")
                
                # Yield result immediately for streaming
                yield res
                
            except Exception as e:
                logger.error(f"❌ Error on {code}: {e}")

    # 5. Finalize
    logger.info("Batch scraping sequence completed.")

if __name__ == "__main__":
    main(limit=2)