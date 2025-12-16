import csv
import json
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class DebugScraper:
    PRINCIPAL_FRAME_URL = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    
    def __init__(self, headless=True):
        self.headless = headless
        self.driver = None
        
    def _setup_driver(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(30)
        
    def _cleanup_driver(self):
        if self.driver:
            self.driver.quit()
            
    def save_debug_info(self, step_name):
        """Save screenshot and HTML for debugging"""
        timestamp = int(time.time())
        try:
            self.driver.save_screenshot(f"debug_{step_name}_{timestamp}.png")
            with open(f"debug_{step_name}_{timestamp}.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            print(f"Saved debug info for: {step_name}")
        except Exception as e:
            print(f"Failed to save debug info for {step_name}: {e}")

    def debug_scrape(self, hs_code):
        print(f"Starting debug scrape for {hs_code}")
        self._setup_driver()
        try:
            self.driver.get(self.PRINCIPAL_FRAME_URL)
            time.sleep(2)
            self.save_debug_info("1_landing")
            
            # Check if we are blocked or on the right page
            if "douane" not in self.driver.page_source.lower() and "adil" not in self.driver.page_source.lower():
                 print("WARNING: Page content does not seem to contain expected keywords.")

            wait = WebDriverWait(self.driver, 15)
            
             # Locate HS code input field (try common patterns)
            input_selectors = [
                "input[name*='code']",
                "input[name*='sh']",
                "input[type='text']",
                "input[id*='code']"
            ]
            
            input_field = None
            for selector in input_selectors:
                try:
                    input_field = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    print(f"Found input with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not input_field:
                print("Could not locate HS code input field")
                return

            input_field.clear()
            input_field.send_keys(hs_code)
            self.save_debug_info("2_input_filled")
            
            # Find and click submit button
            submit_selectors = [
                "input[type='submit']",
                "button[type='submit']",
                "input[value*='Rechercher']",
                "input[value*='Valider']",
                "button"
            ]
            
            submit_button = None
            for selector in submit_selectors:
                try:
                    submit_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    print(f"Found submit button with selector: {selector}")
                    break
                except NoSuchElementException:
                    continue
            
            if submit_button:
                submit_button.click()
            else:
                print("Submitting form directly")
                input_field.submit()
            
            time.sleep(5)
            self.save_debug_info("3_after_submit")
            
            # Check for results
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            print(f"Body text length: {len(body_text)}")
            print("First 500 chars of body text:")
            print(body_text[:500])
            
        except Exception as e:
            print(f"Error during debug scrape: {e}")
            self.save_debug_info("error_state")
        finally:
            self._cleanup_driver()

if __name__ == "__main__":
    scraper = DebugScraper(headless=True)
    scraper.debug_scrape("0804100000")
