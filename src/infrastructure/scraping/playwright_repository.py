"""
Playwright-based implementation of ScrapingRepository
"""

from typing import Optional, List, Dict, Any
import time
import re
import logging

from playwright.sync_api import sync_playwright, Page, Frame, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

from ...domain.entities import TariffCode, ScrapedData, BasicInfo, SectionData, StructuredData, Metadata, SectionType
from ...domain.repositories import ScrapingRepository
from ...domain.value_objects import ScrapingConfiguration, ScrapingStrategy


class PlaywrightScrapingRepository(ScrapingRepository):
    """Playwright-based scraping repository implementation"""
    
    def __init__(self, config: Optional[ScrapingConfiguration] = None):
        self.config = config or ScrapingConfiguration()
        self.strategy_log = {
            'search_frame_strategy': None,
            'content_frame_strategy': None,
            'menu_click_strategy': {},
            'extraction_methods': {},
            'frame_names': [],
            'selectors_used': []
        }
    
    def scrape(self, tariff_code: TariffCode) -> Optional[ScrapedData]:
        """Scrape data for a tariff code"""
        with sync_playwright() as p:
            # Launch browser with better headless support
            browser = p.chromium.launch(
                headless=self.config.headless,
                args=['--disable-blink-features=AutomationControlled'] if self.config.headless else []
            )
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            
            try:
                logger.info(f"Navigating to {self.config.base_url}")
                # Navigate and search
                page.goto(self.config.base_url, wait_until="networkidle", timeout=self.config.timeout)
                self._wait_for_page_ready(page)
                
                # Find search frame
                logger.debug("Finding search frame...")
                search_frame = self._find_search_frame_dynamic(page)
                if not search_frame:
                    logger.warning("Could not find search frame")
                    return None
                logger.debug(f"Found search frame: {search_frame.name}")
                
                # Perform search
                logger.debug(f"Performing search for {tariff_code}...")
                search_success = self._perform_search_dynamic(search_frame, str(tariff_code), page)
                if not search_success:
                    logger.warning("Search failed")
                    return None
                
                logger.debug("Waiting for results...")
                self._wait_for_results_loaded(page)
                
                # Extract data
                scraped_data = ScrapedData(tariff_code_searched=str(tariff_code))
                
                # Extract basic info
                logger.debug("Extracting basic info...")
                main_frame = self._find_content_frame_dynamic(page) or page
                basic_info = self._extract_basic_info(main_frame)
                scraped_data.basic_info = basic_info
                
                # Extract all sections
                logger.info("Extracting sections...")
                sidebar_frame = self._find_sidebar_frame(page)
                if sidebar_frame:
                    menu_items = self._get_menu_items(sidebar_frame)
                    logger.info(f"Found {len(menu_items)} total menu items in sidebar")
                    menu_items_filtered = self._filter_menu_items(menu_items)
                    logger.info(f"After filtering: {len(menu_items_filtered)} sections to scrape")
                    logger.info(f"Section names: {[item['text'] for item in menu_items_filtered]}")
                    
                    for menu_item in menu_items_filtered:
                        section_name = menu_item['text']
                        section_data = self._scrape_section(page, sidebar_frame, section_name)
                        if section_data:
                            scraped_data.add_section(section_data)
                else:
                    logger.warning("Could not find sidebar frame")
                
                if len(scraped_data.sections) == 0:
                    logger.warning("No sections extracted")
                    return None
                
                logger.info(f"Successfully scraped {len(scraped_data.sections)} sections")
                return scraped_data
                
            except Exception as e:
                logger.error(f"Error during scraping: {e}", exc_info=True)
                return None
            finally:
                browser.close()
    
    def scrape_multiple(self, tariff_codes: List[TariffCode]) -> List[ScrapedData]:
        """Scrape multiple tariff codes"""
        results = []
        for tariff_code in tariff_codes:
            result = self.scrape(tariff_code)
            if result:
                results.append(result)
            time.sleep(self.config.delay_between_requests if hasattr(self.config, 'delay_between_requests') else 5)
        return results
    
    def get_strategy_log(self) -> Dict[str, Any]:
        """Get the strategy log for change detection"""
        return self.strategy_log.copy()
    
    # Internal methods (delegated from original scraper)
    def _wait_for_page_ready(self, page: Page, timeout: int = 10000):
        """Wait for page to be ready"""
        try:
            indicators = ['body', 'frame[name*="sommaire"]', 'frame[name*="principal"]', 'input[name="lposition"]']
            for indicator in indicators:
                try:
                    page.wait_for_selector(indicator, timeout=timeout//len(indicators))
                    break
                except:
                    continue
            time.sleep(1)
        except:
            pass
    
    def _find_search_frame_dynamic(self, page: Page, max_retries: int = 3) -> Optional[Frame]:
        """Find search frame using multiple strategies"""
        strategies = [
            ("URL pattern 'c_bas_test_1'", lambda: next((f for f in page.frames if 'c_bas_test_1' in f.url), None)),
            ("Input field presence", lambda: next((f for f in page.frames if self._frame_has_element(f, 'input[name="lposition"]')), None)),
            ("Frame name pattern", lambda: next((f for f in page.frames if 'test' in f.name.lower() or 'search' in f.name.lower()), None)),
            ("Frame with selector", lambda: self._find_frame_with_selector(page, 'input[name="lposition"]')),
        ]
        
        for attempt in range(max_retries):
            for strategy_name, strategy_func in strategies:
                try:
                    frame = strategy_func()
                    if frame and self._validate_frame_has_content(frame, min_text_length=50):
                        self.strategy_log['search_frame_strategy'] = strategy_name
                        self.strategy_log['frame_names'].append(frame.name)
                        return frame
                except:
                    continue
            
            if attempt < max_retries - 1:
                time.sleep(self.config.retry_delay * (attempt + 1))
        
        return None
    
    def _perform_search_dynamic(self, search_frame: Frame, tariff_code: str, page: Page, max_retries: int = 3) -> bool:
        """Perform search with multiple strategies"""
        for attempt in range(max_retries):
            try:
                input_selectors = ['input[name="lposition"]', 'input[type="text"]', 'input#lposition', 'input[name*="position"]']
                
                input_found = False
                for selector in input_selectors:
                    try:
                        input_elem = search_frame.locator(selector).first
                        if input_elem.count() > 0:
                            input_elem.fill(tariff_code)
                            input_found = True
                            self.strategy_log['selectors_used'].append(selector)
                            break
                    except:
                        continue
                
                if not input_found:
                    raise Exception("Could not find input field")
                
                time.sleep(0.5)
                
                submit_selectors = ['input[type="submit"][value*="Trouver"]', 'input[type="submit"]', 'button[type="submit"]', 'input[value*="Trouver"]']
                
                for selector in submit_selectors:
                    try:
                        submit_elem = search_frame.locator(selector).first
                        if submit_elem.count() > 0:
                            submit_elem.click()
                            return True
                    except:
                        continue
                        
            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(self.config.retry_delay)
                    continue
                return False
        
        return False
    
    def _wait_for_results_loaded(self, page: Page, timeout: int = 15000):
        """Wait for search results to load"""
        try:
            indicators = ['text=Position tarifaire', 'text=0804', 'text=ADiL', 'table', 'frame[name*="principal"]']
            for indicator in indicators:
                try:
                    page.wait_for_selector(indicator, timeout=timeout//len(indicators), state='visible')
                    break
                except:
                    continue
            time.sleep(2)
        except:
            time.sleep(3)
    
    def _find_content_frame_dynamic(self, page: Page) -> Optional[Frame]:
        """Find content frame using content scoring"""
        frame_scores = []
        
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                
                score = 0
                text_content = ""
                
                try:
                    body = frame.locator('body')
                    if body.count() > 0:
                        score += 10
                        text_content = body.inner_text(timeout=2000)
                except:
                    pass
                
                if text_content:
                    text_length = len(text_content)
                    if text_length > 500:
                        score += 30
                    elif text_length > 200:
                        score += 20
                    elif text_length > 100:
                        score += 10
                
                try:
                    if frame.locator('table').count() > 0:
                        score += 20
                except:
                    pass
                
                if text_content:
                    if 'Position tarifaire' in text_content or 'ADiL' in text_content:
                        score += 15
                    if re.search(r'\d{4}\.\d{2}\.\d{2}\.\d{2}', text_content):
                        score += 10
                
                frame_name = frame.name.lower()
                if 'principal' in frame_name or 'milieu' in frame_name:
                    score += 5
                
                if any(pattern in frame.url for pattern in ['principal', 'rsearcht', 'r_imp', 'r_exp', 'droite']):
                    score += 10
                
                if score > 0:
                    frame_scores.append((frame, score))
                    
            except:
                continue
        
        if frame_scores:
            frame_scores.sort(key=lambda x: x[1], reverse=True)
            best_frame = frame_scores[0][0]
            strategy_used = "Content scoring"
            if 'principal' in best_frame.name.lower():
                strategy_used = "Frame name 'principal'"
            self.strategy_log['content_frame_strategy'] = strategy_used
            self.strategy_log['frame_names'].append(best_frame.name)
            return best_frame
        
        return None
    
    def _find_sidebar_frame(self, page: Page) -> Optional[Frame]:
        """Find sidebar frame using multiple strategies"""
        # Wait a bit for frames to load
        time.sleep(1)
        
        # Strategy 1: Look for frames with sidebar indicators in URL/name
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                frame_url = frame.url.lower()
                frame_name = frame.name.lower()
                
                # Check for known sidebar patterns
                sidebar_indicators = ['gauche', 'sommaire', 'left', 'sidebar', 'menu', 'navigation']
                if any(indicator in frame_url or indicator in frame_name 
                       for indicator in sidebar_indicators):
                    logger.debug(f"Found sidebar frame by URL/name: {frame.name} ({frame.url[:50]}...)")
                    return frame
            except:
                continue
        
        # Strategy 2: Look for frames with many links (likely sidebar)
        frame_scores = []
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                
                score = 0
                
                # Count links in frame
                try:
                    link_count = frame.evaluate("() => document.querySelectorAll('a').length")
                    if link_count > 5:  # Sidebars typically have many links
                        score += link_count
                        if link_count > 10:
                            score += 20  # Bonus for many links
                except:
                    pass
                
                # Check for sidebar-like content
                try:
                    text = self._get_frame_text(frame)
                    if text:
                        # Sidebars often have navigation-like text
                        if any(keyword in text.lower() for keyword in ['position tarifaire', 'droits', 'importations', 'exportations']):
                            score += 30
                except:
                    pass
                
                # Check frame position/size (sidebars are often narrow)
                try:
                    # Try to get frame dimensions if possible
                    pass
                except:
                    pass
                
                if score > 0:
                    frame_scores.append((frame, score))
                    
            except:
                continue
        
        if frame_scores:
            frame_scores.sort(key=lambda x: x[1], reverse=True)
            best_frame = frame_scores[0][0]
            logger.debug(f"Found sidebar frame by scoring: {best_frame.name} (score: {frame_scores[0][1]})")
            return best_frame
        
        # Strategy 3: List all frames for debugging
        logger.warning("Could not find sidebar frame. Available frames:")
        for frame in page.frames:
            try:
                if not frame.is_detached():
                    logger.warning(f"  - Frame: {frame.name}, URL: {frame.url[:80]}")
            except:
                pass
        
        return None
    
    def _get_menu_items(self, sidebar_frame: Frame) -> List[Dict[str, str]]:
        """Get all menu items from sidebar, including hierarchical/expandable submenus"""
        menu_items = []
        seen_texts = set()  # Track seen items to avoid duplicates
        
        try:
            # Wait a bit for dynamic content to load
            time.sleep(1)
            
            # Strategy 1: Expand all collapsible menus using Playwright locators
            try:
                # Find and click all expandable menu items (those ending with ":")
                expandable_items = sidebar_frame.locator('a').all()
                expandable_texts = []
                
                for item in expandable_items:
                    try:
                        text = item.inner_text().strip()
                        # Check if it looks like an expandable parent menu
                        if text and (text.endswith(':') or text.endswith(' :')) and len(text) < 100:
                            expandable_texts.append(text)
                            try:
                                # Try to click to expand
                                item.click(timeout=2000)
                                time.sleep(0.2)  # Wait for expansion
                            except:
                                # If click fails, try JavaScript click
                                try:
                                    item.evaluate('el => el.click()')
                                    time.sleep(0.2)
                                except:
                                    pass
                    except:
                        continue
                
                if expandable_texts:
                    logger.debug(f"Attempted to expand {len(expandable_texts)} menu items: {expandable_texts[:5]}...")
                    time.sleep(0.5)  # Wait for all expansions to complete
                
                # Now get ALL links (including those revealed by expansion)
                # Get links multiple times to catch any that appear after expansion
                for attempt in range(3):
                    all_links_data = sidebar_frame.evaluate("""
                        () => {
                            const links = Array.from(document.querySelectorAll('a'));
                            return links.map(link => ({
                                text: link.innerText.trim(),
                                href: link.getAttribute('href') || '',
                                visible: link.offsetParent !== null,
                                display: window.getComputedStyle(link).display,
                                parentText: link.parentElement ? link.parentElement.innerText.trim().substring(0, 50) : ''
                            })).filter(item => item.text && item.text.length > 0);
                        }
                    """)
                    
                    if all_links_data:
                        for item in all_links_data:
                            # Skip navigation items
                            if any(skip in item['text'] for skip in ['Nouvelle recherche', 'Version papier']):
                                continue
                            
                            # Skip parent menu items that end with ":" (these are expandable, not sections to scrape)
                            # But keep them if they have actual href (they might be clickable sections)
                            if item['text'].endswith(':') or item['text'].endswith(' :'):
                                # Only skip if it doesn't have a meaningful href
                                if not item['href'] or item['href'] in ['#', 'javascript:void(0)', '']:
                                    continue
                            
                            # Create unique key (use full text to handle duplicates with same name)
                            unique_key = item['text']
                            if unique_key not in seen_texts:
                                seen_texts.add(unique_key)
                                menu_items.append({
                                    'text': item['text'],
                                    'href': item['href']
                                })
                    
                    if attempt < 2:
                        time.sleep(0.3)  # Wait a bit more for any delayed expansions
                
                logger.info(f"Found {len(menu_items)} menu items via DOM query (after expansion attempts)")
            except Exception as e:
                logger.debug(f"DOM query with expansion failed: {e}")
            
            # Strategy 2: Find and scroll scrollable containers
            try:
                # Find scrollable containers (body, divs with overflow, etc.)
                scrollable_containers = sidebar_frame.evaluate("""
                    () => {
                        const containers = [];
                        // Check body
                        if (document.body && (document.body.scrollHeight > document.body.clientHeight)) {
                            containers.push({type: 'body', element: document.body});
                        }
                        // Check all divs with overflow
                        const divs = document.querySelectorAll('div');
                        divs.forEach(div => {
                            const style = window.getComputedStyle(div);
                            if ((style.overflow === 'auto' || style.overflow === 'scroll' || style.overflowY === 'auto' || style.overflowY === 'scroll') 
                                && div.scrollHeight > div.clientHeight) {
                                containers.push({type: 'div', element: div});
                            }
                        });
                        return containers.length;
                    }
                """)
                
                logger.debug(f"Found {scrollable_containers} potentially scrollable containers")
            except Exception as e:
                logger.debug(f"Could not find scrollable containers: {e}")
            
            # Strategy 3: Scroll the frame's document and all scrollable elements
            try:
                initial_count = len(menu_items)
                
                # Try scrolling different elements
                scroll_targets = [
                    ('window', '() => window.scrollBy(0, {step})'),
                    ('body', '() => document.body.scrollBy(0, {step})'),
                    ('documentElement', '() => document.documentElement.scrollBy(0, {step})'),
                ]
                
                # Also try scrolling scrollable divs
                try:
                    scrollable_divs = sidebar_frame.evaluate("""
                        () => {
                            const divs = [];
                            document.querySelectorAll('div').forEach(div => {
                                const style = window.getComputedStyle(div);
                                if ((style.overflow === 'auto' || style.overflow === 'scroll' || style.overflowY === 'auto' || style.overflowY === 'scroll') 
                                    && div.scrollHeight > div.clientHeight) {
                                    divs.push(div);
                                }
                            });
                            return divs.length;
                        }
                    """)
                    logger.debug(f"Found {scrollable_divs} scrollable divs")
                except:
                    pass
                
                # Scroll down gradually using multiple methods
                scroll_step = 200
                max_scrolls = 50
                scroll_count = 0
                last_item_count = len(menu_items)
                no_change_count = 0
                
                while scroll_count < max_scrolls:
                    # Try scrolling window
                    try:
                        sidebar_frame.evaluate(f'() => window.scrollBy(0, {scroll_step})')
                    except:
                        pass
                    
                    # Try scrolling body
                    try:
                        sidebar_frame.evaluate(f'() => document.body.scrollBy(0, {scroll_step})')
                    except:
                        pass
                    
                    # Try scrolling documentElement
                    try:
                        sidebar_frame.evaluate(f'() => document.documentElement.scrollBy(0, {scroll_step})')
                    except:
                        pass
                    
                    # Try scrolling all scrollable divs
                    try:
                        sidebar_frame.evaluate("""
                            () => {
                                document.querySelectorAll('div').forEach(div => {
                                    const style = window.getComputedStyle(div);
                                    if ((style.overflow === 'auto' || style.overflow === 'scroll' || style.overflowY === 'auto' || style.overflowY === 'scroll') 
                                        && div.scrollHeight > div.clientHeight) {
                                        div.scrollBy(0, 200);
                                    }
                                });
                            }
                        """)
                    except:
                        pass
                    
                    time.sleep(0.3)  # Wait for content to load
                    
                    # Get all links after scrolling
                    try:
                        new_links_data = sidebar_frame.evaluate("""
                            () => {
                                const links = Array.from(document.querySelectorAll('a'));
                                return links.map(link => ({
                                    text: link.innerText.trim(),
                                    href: link.getAttribute('href') || ''
                                })).filter(item => item.text && item.text.length > 3);
                            }
                        """)
                        
                        for item in new_links_data:
                            if item['text'] not in seen_texts:
                                seen_texts.add(item['text'])
                                menu_items.append(item)
                    except:
                        pass
                    
                    # Check if we found new items
                    if len(menu_items) > last_item_count:
                        last_item_count = len(menu_items)
                        no_change_count = 0
                        logger.debug(f"Found {len(menu_items)} total items after scroll {scroll_count + 1}")
                    else:
                        no_change_count += 1
                        # If no new items found after 5 scrolls, we're probably done
                        if no_change_count >= 5:
                            logger.debug(f"No new items after {no_change_count} scrolls, stopping")
                            break
                    
                    # Check if we've reached the bottom
                    try:
                        scroll_y = sidebar_frame.evaluate('() => Math.max(window.scrollY || 0, document.documentElement.scrollTop || 0, document.body.scrollTop || 0)')
                        scroll_height = sidebar_frame.evaluate('() => Math.max(document.body.scrollHeight || 0, document.documentElement.scrollHeight || 0)')
                        client_height = sidebar_frame.evaluate('() => Math.max(window.innerHeight || 0, document.documentElement.clientHeight || 0)')
                        
                        if scroll_y + client_height >= scroll_height - 20:  # 20px tolerance
                            logger.debug("Reached bottom of scrollable area")
                            break
                    except:
                        pass
                    
                    scroll_count += 1
                
                # Scroll back to top
                try:
                    sidebar_frame.evaluate('() => { window.scrollTo(0, 0); document.body.scrollTo(0, 0); document.documentElement.scrollTo(0, 0); }')
                    time.sleep(0.3)
                except:
                    pass
                
                if len(menu_items) > initial_count:
                    logger.info(f"Scrolling revealed {len(menu_items) - initial_count} additional menu items (total: {len(menu_items)})")
                else:
                    logger.debug(f"Scrolling did not reveal additional items (still {len(menu_items)})")
                    
            except Exception as e:
                logger.warning(f"Scrolling method failed: {e}")
            
            # Strategy 4: Fallback to locator method (if above methods failed)
            if len(menu_items) == 0:
                try:
                    links = sidebar_frame.locator('a').all()
                    for link in links:
                        try:
                            text = link.inner_text().strip()
                            if text and len(text) > 3 and text not in seen_texts:
                                seen_texts.add(text)
                                menu_items.append({'text': text, 'href': link.get_attribute('href') or ''})
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"Locator method failed: {e}")
            
            logger.info(f"Collected {len(menu_items)} unique menu items from sidebar")
            
        except Exception as e:
            logger.warning(f"Error getting menu items: {e}")
        
        return menu_items
    
    def _filter_menu_items(self, menu_items: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter out navigation items"""
        skip_sections = ['Nouvelle recherche...', 'Version papier']
        return [item for item in menu_items if not any(skip in item['text'] for skip in skip_sections)]
    
    def _scrape_section(self, page: Page, sidebar_frame: Frame, section_name: str) -> Optional[SectionData]:
        """Scrape a single section"""
        try:
            # Find current sidebar
            current_sidebar = None
            for frame in page.frames:
                try:
                    if 'gauche' in frame.url and not frame.is_detached():
                        current_sidebar = frame
                        break
                except:
                    continue
            
            if not current_sidebar:
                return None
            
            # Click menu item
            if not self._click_menu_item_dynamic(current_sidebar, section_name):
                return None
            
            time.sleep(1.5)
            
            # Find content frame
            main_frame = self._find_content_frame_dynamic(page) or page
            
            # Extract section data
            section_data = SectionData(section_name=section_name)
            section_data.structured_data = self._extract_structured_data(main_frame, section_name)
            
            return section_data
            
        except Exception as e:
            return SectionData(section_name=section_name, error=str(e))
    
    def _click_menu_item_dynamic(self, sidebar_frame: Frame, section_name: str, max_retries: int = 3) -> bool:
        """Click menu item using multiple strategies"""
        strategies = [
            ("Exact text match", lambda: sidebar_frame.locator(f'a:has-text("{section_name}")').first),
            ("Text match", lambda: sidebar_frame.locator(f'text="{section_name}"').first),
            ("Partial text match", lambda: sidebar_frame.locator(f'a:has-text("{section_name[:10]}")').first),
            ("Manual link search", lambda: next((link for link in sidebar_frame.locator('a').all() if section_name.lower() in link.inner_text().lower()), None)),
        ]
        
        for attempt in range(max_retries):
            for strategy_name, strategy_func in strategies:
                try:
                    element = strategy_func()
                    if element:
                        element.click(timeout=5000)
                        self.strategy_log['menu_click_strategy'][section_name] = strategy_name
                        return True
                except:
                    continue
            
            if attempt < max_retries - 1:
                time.sleep(0.5)
        
        return False
    
    def _extract_basic_info(self, frame: Frame) -> BasicInfo:
        """Extract basic product information"""
        info = BasicInfo()
        
        try:
            text = self._get_frame_text(frame)
            
            # Tariff code
            tariff_match = re.search(r'(\d{4}\.\d{2}\.\d{2}\.\d{2})', text)
            if tariff_match:
                info.tariff_code = tariff_match.group(1)
            
            # Product description
            desc_patterns = [r'Dattes[^\n]*', r'Description[^\n]*[:：]\s*([^\n]+)']
            for pattern in desc_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    desc = match.group(0) if match.lastindex is None else match.group(1)
                    info.product_description = desc.strip()
                    break
            
            # Effective date
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', text)
            if date_match:
                info.effective_date = date_match.group(1)
            
            # Metadata
            metadata_dict = self._extract_key_value_pairs(frame)
            info.metadata = Metadata(metadata_dict)
            
        except Exception as e:
            info.metadata.set('_error', str(e))
        
        return info
    
    def _extract_structured_data(self, frame: Frame, section_name: str) -> StructuredData:
        """Extract structured data from a section"""
        structured = StructuredData()
        
        try:
            structured.metadata = Metadata(self._extract_key_value_pairs(frame))
            structured.tables = self._extract_structured_tables(frame)
            structured.lists = self._extract_lists(frame)
        except Exception as e:
            structured.metadata.set('_error', str(e))
        
        return structured
    
    def _extract_key_value_pairs(self, frame: Frame) -> Dict[str, Any]:
        """Extract key-value pairs"""
        metadata = {}
        try:
            text = self._get_frame_text(frame)
            patterns = [
                (r'Position\s+tarifaire\s*[:：]\s*([0-9.]+)', 'tariff_position'),
                (r'Période\s+statistique\s*[:：]\s*(.+)', 'statistical_period'),
                (r'Intercom\s*[:：]\s*(.+)', 'intercom'),
                (r'Unité\s+de\s+mesure\s*[:：]\s*(.+)', 'measurement_unit'),
            ]
            for pattern, key in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    metadata[key] = match.group(1).strip()
        except:
            pass
        return metadata
    
    def _extract_structured_tables(self, frame: Frame) -> List[Dict[str, Any]]:
        """Extract structured tables"""
        tables = []
        try:
            table_elements = frame.locator('table').all()
            for table_idx, table in enumerate(table_elements):
                try:
                    rows = table.locator('tr').all()
                    table_data = {'table_index': table_idx + 1, 'headers': [], 'rows': []}
                    
                    if len(rows) == 0:
                        continue
                    
                    first_row_cells = rows[0].locator('td, th').all()
                    if first_row_cells:
                        headers = [cell.inner_text().strip() for cell in first_row_cells if cell.inner_text().strip()]
                        if headers:
                            table_data['headers'] = headers
                            start_row = 1
                        else:
                            start_row = 0
                    else:
                        start_row = 0
                    
                    for row_idx in range(start_row, len(rows)):
                        try:
                            cells = rows[row_idx].locator('td, th').all()
                            row_data = [cell.inner_text().strip() for cell in cells]
                            
                            if row_data and any(cell for cell in row_data):
                                if table_data['headers']:
                                    row_dict = {table_data['headers'][i]: row_data[i] for i in range(min(len(table_data['headers']), len(row_data)))}
                                    table_data['rows'].append(row_dict)
                                else:
                                    table_data['rows'].append(row_data)
                        except:
                            continue
                    
                    if table_data['rows']:
                        tables.append(table_data)
                except:
                    continue
        except:
            pass
        return tables
    
    def _extract_lists(self, frame: Frame) -> List[List[str]]:
        """Extract lists"""
        lists = []
        try:
            list_elements = frame.locator('ul, ol').all()
            for list_elem in list_elements:
                try:
                    items = list_elem.locator('li').all()
                    list_data = [item.inner_text().strip() for item in items if item.inner_text().strip()]
                    if list_data:
                        lists.append(list_data)
                except:
                    continue
        except:
            pass
        return lists
    
    def _get_frame_text(self, frame: Frame) -> str:
        """Extract text from frame"""
        try:
            return frame.locator('body').inner_text(timeout=5000)
        except:
            try:
                return frame.evaluate('() => document.body.textContent || document.documentElement.textContent')
            except:
                try:
                    html = frame.content()
                    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
                    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
                    text = re.sub(r'<[^>]+>', ' ', html)
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text
                except:
                    return ''
    
    def _frame_has_element(self, frame: Frame, selector: str) -> bool:
        """Check if frame has element"""
        try:
            return frame.locator(selector).count() > 0
        except:
            return False
    
    def _validate_frame_has_content(self, frame: Frame, min_text_length: int = 100) -> bool:
        """Validate frame has content"""
        try:
            text = self._get_frame_text(frame)
            return len(text) >= min_text_length
        except:
            return False
    
    def _find_frame_with_selector(self, page: Page, selector: str) -> Optional[Frame]:
        """Find frame with selector"""
        for frame in page.frames:
            try:
                if self._frame_has_element(frame, selector):
                    return frame
            except:
                continue
        return None

