import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import logging
import os
import re
from typing import List, Dict, Optional, Set

# Imports for homepage scraping
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from zoneinfo import ZoneInfo
from gspread.utils import a1_to_rowcol, rowcol_to_a1

# Import from gspread-formatting package
from gspread_formatting import (
    ConditionalFormatRule,
    BooleanCondition,
    CellFormat,
    Color
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StablebondsScraper:
    def __init__(self, credentials_path: str, spreadsheet_url: str, headless: bool = True):
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

    def get_existing_urls(self) -> Set[str]:
        """Gets all existing URLs from Column B to prevent duplicates by parsing HYPERLINK formulas."""
        try:
            logger.info("Fetching existing URLs from the sheet...")
            formulas = self.worksheet.get('B2:B', value_render_option='FORMULA')
            
            urls = set()
            for cell in formulas:
                if not cell: continue
                cell_content = cell[0]
                
                if '=HYPERLINK' in str(cell_content).upper():
                    match = re.search(r'=HYPERLINK\("([^"]+)"', cell_content, re.IGNORECASE)
                    if match:
                        urls.add(match.group(1))
                elif str(cell_content).startswith('http'):
                    urls.add(cell_content)
            
            logger.info(f"Found {len(urls)} existing unique URLs.")
            return urls
        except Exception as e:
            logger.error(f"Error fetching URLs from sheet: {e}")
            return set()

    def scrape_homepage_for_new_bonds(self, existing_urls: Set[str]) -> List[Dict[str, str]]:
        """
        Scrapes the Stablebonds homepage using Selenium to handle dynamic content,
        and returns a list of new, unique bonds based on URL.
        """
        url = "https://stablebonds.in/"
        new_bonds = []
        try:
            logger.info(f"Scraping homepage with Selenium: {url}")
            self.driver.get(url)

            # Wait for the bond container to be present on the page
            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.scrollbar-none a[data-anchor='true'] h4")))
            
            # Now that the page is loaded, parse it with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            
            bond_links = soup.select("div.scrollbar-none a[data-anchor='true']")
            logger.info(f"Found {len(bond_links)} bond links on the homepage.")

            for link in bond_links:
                href = link.get('href')
                if not href or '/bonds/' not in href:
                    continue

                if href not in existing_urls:
                    # *** MODIFIED LOGIC HERE ***
                    # Find the <h4> tag inside the link to get the display name
                    name_tag = link.find('h4')
                    name = name_tag.get_text(strip=True) if name_tag else "Unknown Bond"
                    
                    new_bonds.append({'name': name, 'url': href})
                    existing_urls.add(href)
            
            return new_bonds
        except TimeoutException:
            logger.error("Timed out waiting for bond links to load on the homepage. The page structure might have changed.")
            return []
        except Exception as e:
            logger.error(f"An error occurred during homepage scraping: {e}")
            return []
            
    def get_urls_from_sheet(self) -> List[Dict[str, any]]:
        """Gets all URLs from the sheet for scraping, including row numbers."""
        try:
            logger.info("Fetching all URLs from sheet for detailed scraping...")
            # Get display text from Column A to use in the HYPERLINK formula
            display_texts = self.worksheet.col_values(1)[1:] # Get all names from column A, skipping header
            all_formulas = self.worksheet.get('B2:B', value_render_option='FORMULA')
            
            url_data = []
            for index, formula_cell in enumerate(all_formulas):
                row_num = index + 2
                if not formula_cell: continue
                
                cell_content = formula_cell[0]
                url = None
                
                if '=HYPERLINK' in str(cell_content).upper():
                    match = re.search(r'=HYPERLINK\("([^"]+)"', cell_content, re.IGNORECASE)
                    if match: url = match.group(1)
                elif str(cell_content).startswith('http'):
                    url = cell_content

                if url:
                    # Get the corresponding name from column A for this row
                    display_name = display_texts[index] if index < len(display_texts) else "Link"
                    url_data.append({'row': row_num, 'url': url, 'name': display_name})
            
            logger.info(f"Retrieved {len(url_data)} URLs for detailed scraping.")
            return url_data
        except Exception as e:
            logger.error(f"Error getting URLs from sheet: {e}")
            return []
            
    def scrape_max_value(self, url: str) -> Optional[int]:
        """Scrape the max value from the specified input element using Selenium."""
        try:
            self.driver.get(url)
            time.sleep(4)
            
            selectors = [
                "input.unit-selector-input[type='number']",
                "//aside//input[@type='number']",
                "//input[@type='number' and @inputmode='numeric']"
            ]
            
            target_element = None
            for selector in selectors:
                try:
                    if selector.startswith("//"):
                        element = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector)))
                    else:
                        element = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    target_element = element
                    break
                except TimeoutException:
                    continue
            
            if target_element:
                max_value = target_element.get_attribute('max')
                return int(max_value) if max_value else None
            return None
        except Exception:
            return None
    
    def run_scraping_job(self):
        """Main method to run the complete scraping and updating job."""
        logger.info("Starting scraping job...")
        
        try:
            # === PART 1: DISCOVER AND ADD NEW BONDS ===
            existing_urls = self.get_existing_urls()
            new_bonds = self.scrape_homepage_for_new_bonds(existing_urls)

            if new_bonds:
                logger.info(f"Found {len(new_bonds)} new bonds to add to the sheet.")
                rows_to_append = []
                for bond in new_bonds:
                    # Format: [Name, URL (as formula), Placeholder for Face Value]
                    row = [
                        bond['name'],
                        f'=HYPERLINK("{bond["url"]}", "{bond["name"]}")',
                        '' # Leave Column C blank for manual entry
                    ]
                    rows_to_append.append(row)
                
                self.worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                logger.info("Successfully added new bonds to the Google Sheet.")
            else:
                logger.info("No new bonds found on the homepage.")

            # === PART 2: SCRAPE MAX VALUES FOR ALL BONDS (EXISTING + NEW) ===
            url_infos = self.get_urls_from_sheet()
            if not url_infos:
                logger.warning("No URLs found in the sheet to scrape for details.")
                return

            header_row = self.worksheet.row_values(1)
            next_data_col_index = len([h for h in header_row if h and h.strip()]) + 1

            ist_tz = ZoneInfo("Asia/Kolkata")
            timestamp_str = datetime.now(ist_tz).strftime("%Y-%m-%d %H:%M")
            header_title = f"Data ({timestamp_str})"
            self.worksheet.update_cell(1, next_data_col_index, header_title)
            
            for i, url_info in enumerate(url_infos, 1):
                url, row_num = url_info['url'], url_info['row']
                logger.info(f"Processing {i}/{len(url_infos)}: Scraping details for Row {row_num}")
                max_value = self.scrape_max_value(url)
                value_to_write = max_value if max_value is not None else ""
                self.worksheet.update_cell(row_num, next_data_col_index, value_to_write)
                time.sleep(1) 
            
            # === PART 3: AUTOMATED ANALYSIS ===
            updated_header_row = self.worksheet.row_values(1)
            data_columns = [i for i, h in enumerate(updated_header_row, 1) if h and h.startswith("Data (")]
            
            logger.info(f"Found {len(data_columns)} data columns: {data_columns}")
            
            if len(data_columns) >= 2:
                logger.info("Performing automated analysis...")
                prev_data_col = data_columns[-2]
                curr_data_col = data_columns[-1]
                
                logger.info(f"Comparing column {prev_data_col} with column {curr_data_col}")
                
                all_headers = self.worksheet.row_values(1)
                diff_col_index = len([h for h in all_headers if h.strip()]) + 1
                diff_col_letter = rowcol_to_a1(1, diff_col_index)[:-1]

                prev_values_str = self.worksheet.col_values(prev_data_col, value_render_option='UNFORMATTED_VALUE')[1:]
                curr_values_str = self.worksheet.col_values(curr_data_col, value_render_option='UNFORMATTED_VALUE')[1:]
                face_values_str = self.worksheet.col_values(3, value_render_option='UNFORMATTED_VALUE')[1:] # Using Column C for Face Value

                diff_values = []
                for prev, curr, face_val in zip(prev_values_str, curr_values_str, face_values_str):
                    try:
                        prev_num = float(prev or 0)
                        curr_num = float(curr or 0)
                        face_num = float(face_val or 0)
                        
                        price_diff = prev_num - curr_num
                        total_diff = price_diff * face_num
                        
                        diff_values.append([total_diff])
                    except (ValueError, TypeError):
                        diff_values.append([""])

                diff_header = f"Hourly Change ({timestamp_str})"
                self.worksheet.update_cell(1, diff_col_index, diff_header)
                if diff_values:
                    self.worksheet.update(diff_values, f'{diff_col_letter}2')
                logger.info(f"Added '{diff_header}' column at index {diff_col_index}.")
            else:
                logger.info("Need at least 2 data columns to calculate hourly changes.")

            logger.info("Scraping job completed successfully.")
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed.")

def main():
    """Main function to run the scraper"""
    CREDENTIALS_PATH = 'service_account.json'
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        scraper = StablebondsScraper(CREDENTIALS_PATH, SPREADSHEET_URL, headless=True)
        scraper.run_scraping_job()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'.")
    except Exception as e:
        logger.error(f"An error occurred during the main execution: {e}")

if __name__ == "__main__":
    main()
