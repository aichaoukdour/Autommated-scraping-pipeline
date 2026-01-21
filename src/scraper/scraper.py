import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import asdict

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from .config import ScraperConfig, logger
from .models import ScrapeResult
from .parsing import TextProcessor
from .browser import WebDriverManager

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
        
        input_field = self.wait.until(EC.presence_of_element_located((By.NAME, "lposition")))
        input_field.clear()
        input_field.send_keys(hs_code)
        
        submit_btn = self.driver.find_element(By.NAME, "submit")
        submit_btn.click()
        
        try:
            self.wait.until(EC.frame_to_be_available_and_switch_to_it(2))
            self.driver.switch_to.default_content()
        except TimeoutException:
            raise Exception("Search yielded no results or timed out.")
    
    def _scrape_main_content(self, result: ScrapeResult) -> None:
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2)
            
            html_content = self.driver.find_element(By.TAG_NAME, "body").get_attribute("outerHTML")
            
            content = self.processor.process_content(html_content)
            result.main_content = asdict(content)
            
        except Exception as e:
            logger.warning(f"Main content scrape failed: {e}")
            result.scrape_status = "partial"

    def _scrape_all_sections(self, result: ScrapeResult) -> None:
        section_links = self._get_section_links()
        
        for idx, link_info in enumerate(section_links):
            self._process_single_section(result, link_info, idx)

    def _get_section_links(self) -> List[Dict[str, str]]:
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(1)
        
        links = self.driver.find_elements(By.TAG_NAME, "a")
        section_links = []
        seen = set()
        
        for link in links:
            txt = link.get_attribute("textContent").strip()
            txt = " ".join(txt.split())
            
            if txt and txt not in seen and not any(k in txt.lower() for k in self.SKIP_KEYWORDS):
                section_links.append({"name": txt})
                seen.add(txt)
                
        return section_links

    def _process_single_section(self, result: ScrapeResult, link_info: Dict[str, str], idx: int):
        section_name = link_info["name"]
        
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(1)
            
            links = self.driver.find_elements(By.TAG_NAME, "a")
            target = next((l for l in links if " ".join(l.text.split()) == section_name), None)
            
            if not target:
                logger.warning(f"Link lost: {section_name}")
                return

            self.driver.execute_script("arguments[0].click();", target)
            time.sleep(self.config.section_load_delay)
            
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2)
            
            body = self.driver.find_element(By.TAG_NAME, "body")
            html_content = body.get_attribute("outerHTML")
            
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
