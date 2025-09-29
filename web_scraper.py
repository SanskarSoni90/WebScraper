import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import logging
import os
from typing import List, Dict, Optional
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from zoneinfo import ZoneInfo

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
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(self.credentials_path, scopes=scope)
            self.gc = gspread.authorize(creds)
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
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            logger.info("Selenium WebDriver setup successful")
        except Exception as e:
            logger.error(f"Error setting up Selenium: {e}")
            raise
    
    def get_urls_from_sheet(self) -> List[Dict[str, any]]:
        """
        Get all URLs and their corresponding row numbers from column B using an efficient batch request.
        
        Returns:
            A list of dictionaries, where each dictionary contains a 'row' and 'url'.
        """
        try:
            logger.info("Fetching all URLs from sheet in a single batch request...")
            # BATCH GET: Get all formulas from column B (starting at row 2) in ONE API call.
            # Change 'B2:B' if your URLs are in a different column.
            all_formulas = self.worksheet.get('B2:B', value_render_option='FORMULA')
            
            url_data = []
            # The loop now runs on data already in memory, making no new API calls.
            for index, formula_cell in enumerate(all_formulas):
                row_num = index + 2  # +2 because our range starts from row 2
                
                # The result is a list within a list, e.g., [['=HYPERLINK(...)']] or [['https...']]
                if not formula_cell:
                    continue # Skip empty rows
                
                cell_content = formula_cell[0]
                url = None
                
                if '=HYPERLINK' in str(cell_content).upper():
                    match = re.search(r'=HYPERLINK\("([^"]+)"', cell_content, re.IGNORECASE)
                    if match:
                        url = match.group(1)
                elif str(cell_content).startswith('http'):
                    url = cell_content

                if url:
                    url_data.append({'row': row_num, 'url': url})
            
            logger.info(f"Retrieved {len(url_data)} URLs from the sheet.")
            return url_data
            
        except Exception as e:
            logger.error(f"Error getting URLs from sheet: {e}")
            return []
            
    def scrape_max_value(self, url: str) -> Optional[int]:
        """
        Scrape the max value from the specified input element using Selenium.
        
        Args:
            url: URL to scrape
            
        Returns:
            Max value as an integer or None if not found.
        """
        try:
            self.driver.get(url)
            time.sleep(3) # Wait for page to load and JS to execute
            
            selectors = [
                "input.unit-selector-input.border-black-20[type='number']",
                "input.unit-selector-input[type='number']",
                "input[inputmode='numeric'][type='number']",
                "//input[contains(@class, 'unit-selector-input')]",
                "//aside//input[@type='number']",
                "//input[@type='number' and @inputmode='numeric']"
            ]
            
            target_element = None
            for selector in selectors:
                try:
                    if selector.startswith("//"):
                        element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                    else:
                        element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                    target_element = element
                    break
                except TimeoutException:
                    continue
            
            if target_element:
                max_value = target_element.get_attribute('max')
                if max_value is not None:
                    try:
                        max_int = int(max_value)
                        logger.info(f"Successfully extracted max value: {max_int} from {url}")
                        return max_int
                    except (ValueError, TypeError):
                        logger.error(f"Max attribute '{max_value}' is not a valid integer for {url}")
                        return None
                else:
                    logger.warning(f"Found target element but no 'max' attribute for {url}")
                    return None
            else:
                logger.warning(f"Target input element not found for {url}")
                return None
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def run_scraping_job(self):
        """Main method to run the complete scraping and updating job."""
        logger.info("Starting scraping job...")
        
        try:
            # Get URLs and their row numbers from the sheet
            url_infos = self.get_urls_from_sheet()
            if not url_infos:
                logger.error("No URLs found in the sheet. Exiting.")
                return

            # Find the next available column to write data to
            header_row = self.worksheet.row_values(1)
            next_col_index = len(header_row) + 1

            # Define the Indian Standard Time timezone
            ist_tz = ZoneInfo("Asia/Kolkata")
            
            # Create a timestamped header for the new column
            timestamp_str = datetime.now(ist_tz).strftime("%Y-%m-%d %H:%M")
            header_title = f"Indian Timestamp ({timestamp_str})"
            self.worksheet.update_cell(1, next_col_index, header_title)
            logger.info(f"Created new column with header: '{header_title}' at column index {next_col_index}")

            # Scrape each URL and update the sheet row by row
            for i, url_info in enumerate(url_infos, 1):
                url = url_info['url']
                row_num = url_info['row']
                
                logger.info(f"Processing {i}/{len(url_infos)}: Row {row_num}, URL {url}")
                
                max_value = self.scrape_max_value(url)
                
                # Prepare value for sheet: empty string if not found, otherwise the number
                value_to_write = max_value if max_value is not None else ""
                
                # Update the specific cell in the new column
                self.worksheet.update_cell(row_num, next_col_index, value_to_write)
                
                # Add a polite delay to avoid overwhelming the server
                time.sleep(2)
            
            logger.info("Scraping job completed successfully.")
            
        finally:
            # Always ensure the browser is closed
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed.")

def main():
    """Main function to run the scraper"""
    # --- CONFIGURATION ---
    # Make sure 'service_account.json' is in the same directory, or provide the full path.
    CREDENTIALS_PATH = 'service_account.json'
    # Replace with your actual Google Sheet URL
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        # Initialize and run the scraper
        # Set headless=False if you want to see the browser window during execution
        scraper = SeleniumWebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL, headless=True)
        scraper.run_scraping_job()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'. Please ensure the file exists.")
    except Exception as e:
        logger.error(f"An error occurred during the main execution: {e}")

if __name__ == "__main__":
    main()
