import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import logging
import os
from typing import List, Dict, Optional
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumWebScraperGoogleSheets:
    def __init__(self, credentials_path: str, spreadsheet_url: str, headless: bool = True):
        """
        Initialize the scraper with Google Sheets credentials
        
        Args:
            credentials_path: Path to Google service account JSON file
            spreadsheet_url: URL of the Google Sheet
            headless: Whether to run browser in headless mode
        """
        self.credentials_path = credentials_path
        self.spreadsheet_url = spreadsheet_url
        self.headless = headless
        self.gc = None
        self.worksheet = None
        self.driver = None
        self.setup_google_sheets()
        self.setup_selenium()
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            # Define the scope
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Load credentials
            creds = Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=scope
            )
            
            # Initialize the client
            self.gc = gspread.authorize(creds)
            
            # Open the spreadsheet
            self.worksheet = self.gc.open_by_url(self.spreadsheet_url).sheet1
            logger.info("Google Sheets connection established successfully")
            
        except Exception as e:
            logger.error(f"Error setting up Google Sheets: {e}")
            raise
    
    def setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            # For GitHub Actions or Docker environments
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-images')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            logger.info("Selenium WebDriver setup successful")
            
        except Exception as e:
            logger.error(f"Error setting up Selenium: {e}")
            raise
    
    def get_urls_from_sheet(self) -> List[str]:
        """Get all URLs from column B hyperlinks of the sheet"""
        try:
            # Get all cells in column B with their rich text data
            cell_list = self.worksheet.range('B2:B1000')  # Adjust range as needed
            urls = []
            
            for cell in cell_list:
                if not cell.value:  # Skip empty cells
                    continue
                    
                # Try to get hyperlink from the cell
                try:
                    # Get the cell with formula to check for HYPERLINK function
                    cell_formula = self.worksheet.cell(cell.row, cell.col, value_render_option='FORMULA').value
                    
                    if cell_formula and '=HYPERLINK(' in cell_formula:
                        # Extract URL from HYPERLINK formula: =HYPERLINK("URL","text")
                        match = re.search(r'=HYPERLINK\("([^"]+)"', cell_formula)
                        if match:
                            url = match.group(1)
                            urls.append(url)
                            logger.info(f"Row {cell.row}: Found URL {url}")
                        else:
                            logger.warning(f"Row {cell.row}: Could not extract URL from formula: {cell_formula}")
                    elif cell.value.startswith('http'):
                        # Direct URL
                        urls.append(cell.value)
                        logger.info(f"Row {cell.row}: Direct URL {cell.value}")
                    else:
                        logger.warning(f"Row {cell.row}: No hyperlink found for value: {cell.value}")
                        
                except Exception as e:
                    logger.warning(f"Row {cell.row}: Error extracting hyperlink: {e}")
                    continue
            
            # Remove empty entries and duplicates
            urls = list(filter(None, urls))
            urls = list(dict.fromkeys(urls))  # Remove duplicates while preserving order
            
            logger.info(f"Retrieved {len(urls)} unique URLs from sheet")
            return urls
            
        except Exception as e:
            logger.error(f"Error getting URLs from sheet: {e}")
            return []
    
    def scrape_max_value(self, url: str, debug_mode: bool = False) -> Optional[int]:
        """
        Scrape the max value from the specified input element using Selenium
        
        Args:
            url: URL to scrape
            debug_mode: If True, will print detailed debug information
            
        Returns:
            Max value as integer or None if not found
        """
        try:
            # Navigate to the URL
            self.driver.get(url)
            
            # Wait for the page to load and JavaScript to execute
            time.sleep(3)
            
            # Try multiple selectors to find the target element
            selectors = [
                # CSS selector based on classes
                "input.unit-selector-input.border-black-20[type='number']",
                "input.unit-selector-input[type='number']",
                "input[inputmode='numeric'][type='number']",
                # XPath selectors
                "//input[contains(@class, 'unit-selector-input')]",
                "//aside//input[@type='number']",
                "//input[@type='number' and @inputmode='numeric']"
            ]
            
            target_element = None
            method_used = None
            
            for i, selector in enumerate(selectors):
                try:
                    if selector.startswith("//"):
                        # XPath selector
                        element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                    else:
                        # CSS selector
                        element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                    
                    target_element = element
                    method_used = f"selector {i+1}: {selector}"
                    if debug_mode:
                        print(f"Found element using {method_used}")
                    break
                    
                except TimeoutException:
                    if debug_mode:
                        print(f"Selector {i+1} timed out: {selector}")
                    continue
                except Exception as e:
                    if debug_mode:
                        print(f"Selector {i+1} failed: {selector} - {e}")
                    continue
            
            if target_element:
                # Get the max attribute
                max_value = target_element.get_attribute('max')
                
                if debug_mode:
                    print(f"\nFound target element using: {method_used}")
                    print(f"Element tag: {target_element.tag_name}")
                    print(f"Element classes: {target_element.get_attribute('class')}")
                    print(f"Max attribute: {max_value}")
                    print(f"Value attribute: {target_element.get_attribute('value')}")
                    print(f"Min attribute: {target_element.get_attribute('min')}")
                    print(f"Type attribute: {target_element.get_attribute('type')}")
                
                if max_value is not None:
                    try:
                        max_int = int(max_value)
                        logger.info(f"Successfully extracted max value: {max_int} from {url} using {method_used}")
                        return max_int
                    except (ValueError, TypeError):
                        logger.error(f"Max attribute '{max_value}' is not a valid integer for {url}")
                        return None
                else:
                    logger.warning(f"Found target element but no 'max' attribute for {url}")
                    return None
            else:
                logger.warning(f"Target input element not found for {url}")
                
                if debug_mode:
                    # Show all input elements for debugging
                    all_inputs = self.driver.find_elements(By.TAG_NAME, 'input')
                    print(f"\nAll {len(all_inputs)} input elements found:")
                    for i, inp in enumerate(all_inputs[:10]):  # Show first 10
                        try:
                            print(f"  {i+1}: type={inp.get_attribute('type')}, "
                                  f"class={inp.get_attribute('class')}, "
                                  f"max={inp.get_attribute('max')}, "
                                  f"name={inp.get_attribute('name')}")
                        except Exception as e:
                            print(f"  {i+1}: Error getting attributes - {e}")
                
                return None
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def update_sheet_with_values(self, values: List[int], timestamp: str):
        """
        Update the sheet with scraped values in a new column
        
        Args:
            values: List of max values scraped
            timestamp: Current timestamp for column header
        """
        try:
            # Find the next available column
            all_values = self.worksheet.get_all_values()
            if all_values:
                next_col_index = len(all_values[0]) + 1
            else:
                next_col_index = 3  # Start from column C if sheet is empty
            
            # Convert column index to letter
            col_letter = self.index_to_column_letter(next_col_index)
            
            # Update header with timestamp
            self.worksheet.update(f'{col_letter}1', timestamp)
            
            # Update values starting from row 2
            if values:
                # Prepare data for batch update
                data = [[value] for value in values]
                range_name = f'{col_letter}2:{col_letter}{len(values) + 1}'
                self.worksheet.update(range_name, data)
            
            logger.info(f"Updated sheet with {len(values)} values in column {col_letter}")
            
        except Exception as e:
            logger.error(f"Error updating sheet: {e}")
    
    def index_to_column_letter(self, index: int) -> str:
        """Convert column index to letter (1=A, 2=B, etc.)"""
        result = ""
        while index > 0:
            index -= 1
            result = chr(ord('A') + (index % 26)) + result
            index //= 26
        return result
    
    def run_scraping_job(self, debug_mode: bool = False):
        """Main method to run the complete scraping job"""
        logger.info("Starting scraping job...")
        
        try:
            # Get current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
            
            # Get URLs from sheet
            urls = self.get_urls_from_sheet()
            if not urls:
                logger.error("No URLs found in sheet")
                return
            
            # Scrape max values
            max_values = []
            for i, url in enumerate(urls, 1):
                logger.info(f"Scraping URL {i}/{len(urls)}: {url}")
                max_value = self.scrape_max_value(url, debug_mode=debug_mode)
                max_values.append(max_value if max_value is not None else "")
                
                # Add delay to be respectful to servers
                time.sleep(2)
            
            # Update sheet with results
            self.update_sheet_with_values(max_values, timestamp)
            logger.info("Scraping job completed successfully")
            
        finally:
            # Always close the browser
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")

def debug_single_url_selenium():
    """Debug a single URL with Selenium"""
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        # Initialize scraper (set headless=False to see the browser)
        scraper = SeleniumWebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL, headless=True)
        test_url = "https://stablebonds.in/bonds/ugro-capital-limited/INE583D07570"
        
        # Try to scrape with debug mode
        max_val = scraper.scrape_max_value(test_url, debug_mode=True)
        print(f"\nFinal result: {max_val}")
        
    except Exception as e:
        logger.error(f"Error in debug: {e}")
    finally:
        if 'scraper' in locals() and scraper.driver:
            scraper.driver.quit()

def main():
    """Main function to run the scraper"""
    # Configuration
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        # Initialize scraper
        scraper = SeleniumWebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL, headless=True)
        
        # For debugging, uncomment the line below
        # debug_single_url_selenium()
        # return
        
        # Run scraping job with debug mode for first few URLs
        scraper.run_scraping_job(debug_mode=True)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    # For immediate debugging, run this:
    debug_single_url_selenium()
    
    # For normal operation, run this instead:
    # main()
