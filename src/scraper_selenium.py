"""
Selenium-based scraper for ADIL (Moroccan Customs) website.
Uses headless Chrome with proper wait for nested frames.
"""
import time
import json
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.douane.gov.ma/adil/"
INPUT_CSV = "Code Sh Import - Feuille 1.csv"
OUTPUT_CSV = "products_data.csv"
OUTPUT_JSON = "products_data.json"
SAMPLE_SIZE = 3  # Small sample for testing

# Setup logging
logging.basicConfig(
    filename="scraper_selenium.log",
    filemode='w',
    level=logging.DEBUG,  # Debug level for more info
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


def pad_hs_code(hs_code: str) -> str:
    """Pad HS code to 10 digits."""
    hs_code = str(hs_code).strip()
    if len(hs_code) < 6:
        hs_code = hs_code.zfill(6)
    if len(hs_code) < 10:
        hs_code = hs_code.ljust(10, '0')
    return hs_code


def create_driver():
    """Create headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    driver.set_page_load_timeout(60)
    return driver


def explore_frames(driver, depth=0, max_depth=3):
    """Recursively explore and log all frames."""
    if depth > max_depth:
        return []
    
    indent = "  " * depth
    frame_info = []
    
    frames = driver.find_elements(By.TAG_NAME, "frame")
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    all_frames = frames + iframes
    
    for i, frame in enumerate(all_frames):
        name = frame.get_attribute("name") or f"unnamed_{i}"
        src = frame.get_attribute("src") or "no_src"
        info = {"name": name, "src": src, "element": frame, "children": []}
        logging.debug(f"{indent}Frame: name='{name}', src='{src[:60]}'")
        
        try:
            driver.switch_to.frame(frame)
            time.sleep(0.5)
            info["children"] = explore_frames(driver, depth + 1, max_depth)
            driver.switch_to.parent_frame()
        except Exception as e:
            logging.debug(f"{indent}  Could not enter frame: {e}")
        
        frame_info.append(info)
    
    return frame_info


def scrape_hs_code(driver, wait, hs_code: str) -> dict:
    """Scrape duty rates for a single HS code."""
    hs_code_padded = pad_hs_code(hs_code)
    logging.info(f"Processing: {hs_code} -> {hs_code_padded}")
    
    result = {
        "hs_code_input": hs_code_padded,
        "description": "N/A",
        "di_rate": "N/A",
        "tpi_rate": "N/A",
        "tva_rate": "N/A"
    }
    
    try:
        driver.get(BASE_URL)
        time.sleep(4)  # Longer wait for full page load
        
        if "Request Rejected" in driver.page_source:
            logging.error("WAF blocked!")
            return result
        
        logging.info(f"Title: {driver.title}")
        
        driver.switch_to.default_content()
        
        # Explore all frames
        logging.debug("Exploring frame structure...")
        frame_tree = explore_frames(driver)
        
        driver.switch_to.default_content()
        
        # The 'principal' frame contains another frameset
        # Let's navigate to it and wait for the inner content
        try:
            principal = driver.find_element(By.NAME, "principal")
            driver.switch_to.frame(principal)
            logging.info("In 'principal' frame")
            
            # Wait for the frameset to load inside principal
            time.sleep(2)
            
            # Get the page source to see what's inside
            html = driver.page_source
            logging.debug(f"Principal frame HTML (first 500): {html[:500]}")
            
            # Check if it's a frameset
            if "<frameset" in html.lower():
                logging.info("Principal contains a frameset!")
                
                # Find inner frames
                inner_frames = driver.find_elements(By.TAG_NAME, "frame")
                logging.info(f"Found {len(inner_frames)} inner frames")
                
                for i, frame in enumerate(inner_frames):
                    name = frame.get_attribute("name") or ""
                    src = frame.get_attribute("src") or ""
                    logging.info(f"  Inner frame {i}: name='{name}', src='{src[:50]}'")
                
                # Find search/gauche frame
                for frame in inner_frames:
                    name = frame.get_attribute("name") or ""
                    src = frame.get_attribute("src") or ""
                    if "gauche" in name.lower() or "rsearch" in src.lower():
                        driver.switch_to.frame(frame)
                        logging.info(f"Switched to search frame: {name}")
                        
                        # Find search input
                        search_input = wait.until(EC.presence_of_element_located((By.NAME, "lposition")))
                        search_input.clear()
                        search_input.send_keys(hs_code_padded)
                        
                        submit = driver.find_element(By.NAME, "submit")
                        submit.click()
                        logging.info("Search submitted")
                        
                        time.sleep(3)
                        
                        # Switch to rates frame
                        driver.switch_to.default_content()
                        driver.switch_to.frame(driver.find_element(By.NAME, "principal"))
                        time.sleep(1)
                        
                        inner_frames = driver.find_elements(By.TAG_NAME, "frame")
                        for f in inner_frames:
                            src = f.get_attribute("src") or ""
                            if "info_2" in src:
                                driver.switch_to.frame(f)
                                html = driver.page_source
                                soup = BeautifulSoup(html, 'html.parser')
                                
                                for elem in soup.find_all(['span', 'b']):
                                    text = elem.get_text(strip=True)
                                    if '%' in text:
                                        parent = elem.find_parent('tr')
                                        if parent:
                                            row = parent.get_text()
                                            if 'DI' in row and result['di_rate'] == 'N/A':
                                                result['di_rate'] = text
                                            elif 'TPI' in row and result['tpi_rate'] == 'N/A':
                                                result['tpi_rate'] = text
                                            elif 'TVA' in row and result['tva_rate'] == 'N/A':
                                                result['tva_rate'] = text
                                break
                        break
            else:
                # No frameset, maybe direct content
                logging.info("No frameset in principal, checking for search form")
                
                # Try to find search input directly
                try:
                    search_input = wait.until(EC.presence_of_element_located((By.NAME, "lposition")))
                    search_input.clear()
                    search_input.send_keys(hs_code_padded)
                    submit = driver.find_element(By.NAME, "submit")
                    submit.click()
                    time.sleep(3)
                except:
                    # Save page source for debug
                    with open(f"debug_{hs_code_padded}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    logging.warning(f"Saved debug HTML for {hs_code_padded}")
                    
        except NoSuchElementException:
            logging.error("No 'principal' frame found")
        
        logging.info(f"Result: DI={result['di_rate']}, TPI={result['tpi_rate']}, TVA={result['tva_rate']}")
        
    except Exception as e:
        logging.error(f"Error: {e}")
    
    return result


def main():
    """Main function."""
    print("=" * 60)
    print("ADIL Selenium Scraper (Headless)")
    print("=" * 60)
    
    df = pd.read_csv(INPUT_CSV, dtype=str)
    hs_codes = df.iloc[:, 0].tolist()
    print(f"Loaded {len(hs_codes)} codes")
    
    print("Starting headless Chrome...")
    driver = create_driver()
    wait = WebDriverWait(driver, 15)
    
    results = []
    codes = hs_codes[:SAMPLE_SIZE] if SAMPLE_SIZE else hs_codes
    
    try:
        for i, hs_code in enumerate(codes):
            print(f"[{i+1}/{len(codes)}] {hs_code}...")
            result = scrape_hs_code(driver, wait, hs_code)
            results.append(result)
            time.sleep(1)
    finally:
        driver.quit()
    
    pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    print(f"Saved {len(results)} records")


if __name__ == "__main__":
    main()
