from selenium import webdriver
from .config import ScraperConfig

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
