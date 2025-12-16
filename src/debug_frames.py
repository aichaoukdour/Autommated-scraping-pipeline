"""
Debug Frames
Inspects frames after search and saves their content.
"""
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def debug_frames():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        print("1. Priming Session...")
        driver.get("https://www.douane.gov.ma/adil/")
        time.sleep(5)

        # Go to principal
        driver.switch_to.frame("principal")

        # Search
        print("2. Searching for 0101210000...")
        search = wait.until(EC.presence_of_element_located((By.NAME, "lposition")))
        search.clear()
        search.send_keys("0101210000")
        driver.find_element(By.NAME, "submit").click()
        time.sleep(5)
        
        print("3. Inspecting Frames AFTER Search...")
        # We are still in 'principal' (unless page reloaded top level, which is unlikely for frameset)
        # Verify we are in principal or switch back if needed
        driver.switch_to.default_content()
        driver.switch_to.frame("principal")
        
        frames = driver.find_elements(By.TAG_NAME, "frame")
        print(f"Found {len(frames)} frames in 'principal'")
        
        for i, f in enumerate(frames):
            name = f.get_attribute("name")
            src = f.get_attribute("src")
            print(f"  Frame {i}: Name='{name}', Src='{src}'")
            
            # Save content of likely data frames
            if name in ["milieu", "droite"] or "result" in src.lower():
                print(f"    -> Saving content of '{name}'...")
                driver.switch_to.frame(f)
                with open(f"debug_frame_{name}.html", "w", encoding="utf-8") as file:
                    file.write(driver.page_source)
                driver.switch_to.parent_frame()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_frames()
