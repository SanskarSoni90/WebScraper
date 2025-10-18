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

    def get_existing_bonds(self) -> tuple[Set[str], Set[str]]:
        """Gets all existing bond names and URLs to prevent duplicates."""
        try:
            logger.info("Fetching existing bond names and URLs from the sheet...")
            
            # Get all data from columns A and B
            all_data = self.worksheet.get('A2:B', value_render_option='FORMULA')
            
            bond_names = set()
            urls = set()
            skipped_count = 0
            
            for index, row in enumerate(all_data):
                row_num = index + 2
                
                # Skip completely empty rows
                if not row or len(row) == 0:
                    skipped_count += 1
                    continue
                
                # Get name from column A
                if len(row) > 0 and row[0]:
                    name = str(row[0]).strip()
                    if name:
                        bond_names.add(name.lower())  # Store lowercase for case-insensitive comparison
                
                # Get URL from column B
                if len(row) > 1 and row[1]:
                    cell_content = row[1]
                    
                    if '=HYPERLINK' in str(cell_content).upper():
                        match = re.search(r'=HYPERLINK\("([^"]+)"', cell_content, re.IGNORECASE)
                        if match:
                            urls.add(match.group(1))
                    elif str(cell_content).startswith('http'):
                        urls.add(cell_content)
            
            logger.info(f"Found {len(bond_names)} existing unique bond names and {len(urls)} existing unique URLs (skipped {skipped_count} empty rows).")
            
            return bond_names, urls
        except Exception as e:
            logger.error(f"Error fetching bond data from sheet: {e}")
            return set(), set()

    def scrape_homepage_for_new_bonds(self, existing_names: Set[str], existing_urls: Set[str]) -> List[Dict[str, str]]:
        """
        Scrapes the Stablebonds homepage using Selenium to handle dynamic content,
        and returns a list of new, unique bonds based on both name and URL.
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

                # Find the <h4> tag inside the link to get the display name
                name_tag = link.find('h4')
                name = name_tag.get_text(strip=True) if name_tag else "Unknown Bond"
                
                # Check both URL and name for duplicates
                is_duplicate_url = href in existing_urls
                is_duplicate_name = name.lower() in existing_names
                
                if is_duplicate_url and is_duplicate_name:
                    logger.debug(f"Skipping duplicate bond: {name} (both name and URL exist)")
                    continue
                elif is_duplicate_url:
                    logger.warning(f"URL exists but name differs: '{name}' - URL: {href}")
                    # Still skip if URL exists, even if name is different
                    continue
                elif is_duplicate_name:
                    logger.warning(f"Name exists but URL differs: '{name}' - New URL: {href}")
                    # Skip if name exists to prevent duplicates in Column A
                    continue
                else:
                    # This is a genuinely new bond
                    new_bonds.append({'name': name, 'url': href})
                    existing_urls.add(href)
                    existing_names.add(name.lower())
                    logger.info(f"Found new bond: {name}")
            
            return new_bonds
        except TimeoutException:
            logger.error("Timed out waiting for bond links to load on the homepage. The page structure might have changed.")
            return []
        except Exception as e:
            logger.error(f"An error occurred during homepage scraping: {e}")
            return []
    
    def get_last_data_row(self) -> int:
        """Get the last row that contains data in column A or B"""
        try:
            col_a_values = self.worksheet.col_values(1)  # Column A
            col_b_values = self.worksheet.col_values(2)  # Column B
            
            # Find the last non-empty row in either column A or B
            last_row = max(len(col_a_values), len(col_b_values))
            
            # Clean up: find actual last row with meaningful data
            while last_row > 1:  # Don't go above header
                if (last_row <= len(col_a_values) and col_a_values[last_row - 1].strip()) or \
                   (last_row <= len(col_b_values) and col_b_values[last_row - 1].strip()):
                    break
                last_row -= 1
            
            logger.info(f"Last data row found at: {last_row}")
            return last_row
        except Exception as e:
            logger.error(f"Error finding last data row: {e}")
            return 1  # Return header row if error
            
    def get_urls_from_sheet(self) -> List[Dict[str, any]]:
        """Gets all URLs from the sheet for scraping, including row numbers."""
        try:
            logger.info("Fetching all URLs from sheet for detailed scraping...")
            
            # Get all data from columns A and B at once
            all_data = self.worksheet.get('A2:B', value_render_option='FORMULA')
            
            url_data = []
            for index, row in enumerate(all_data):
                row_num = index + 2
                
                # Skip completely empty rows
                if not row or len(row) == 0:
                    continue
                
                # Get name from column A
                display_name = row[0] if len(row) > 0 and row[0] else f"Bond {row_num}"
                
                # Get URL from column B
                if len(row) > 1 and row[1]:
                    cell_content = row[1]
                    url = None
                    
                    if '=HYPERLINK' in str(cell_content).upper():
                        match = re.search(r'=HYPERLINK\("([^"]+)"', cell_content, re.IGNORECASE)
                        if match: 
                            url = match.group(1)
                    elif str(cell_content).startswith('http'):
                        url = cell_content
                    
                    if url:
                        url_data.append({'row': row_num, 'url': url, 'name': display_name})
                        logger.debug(f"Row {row_num}: {display_name} -> {url}")
                else:
                    logger.warning(f"Row {row_num} has name '{display_name}' but no URL in column B")
            
            logger.info(f"Retrieved {len(url_data)} URLs for detailed scraping.")
            
            # Additional check: Get total non-empty rows in column B
            all_col_b = self.worksheet.col_values(2)
            non_empty_b = [cell for cell in all_col_b[1:] if cell and cell.strip()]
            logger.info(f"Total non-empty cells in column B: {len(non_empty_b)}")
            
            if len(url_data) < len(non_empty_b):
                logger.warning(f"Mismatch detected! Found {len(url_data)} parseable URLs but {len(non_empty_b)} non-empty cells in column B")
            
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
            existing_names, existing_urls = self.get_existing_bonds()
            new_bonds = self.scrape_homepage_for_new_bonds(existing_names, existing_urls)

            if new_bonds:
                logger.info(f"Found {len(new_bonds)} new bonds to add to the sheet.")
                
                # Find the next available row after existing data
                last_row = self.get_last_data_row()
                next_row = last_row + 1
                
                logger.info(f"Adding new bonds starting from row {next_row}")
                
                # Add each new bond individually to ensure proper placement
                for i, bond in enumerate(new_bonds):
                    row_to_write = next_row + i
                    
                    # Write to column A (Name)
                    self.worksheet.update_cell(row_to_write, 1, bond['name'])
                    
                    # Write to column B (URL as hyperlink)
                    hyperlink_formula = f'=HYPERLINK("{bond["url"]}", "{bond["name"]}")'
                    self.worksheet.update_cell(row_to_write, 2, hyperlink_formula)
                    
                    logger.info(f"Added '{bond['name']}' at row {row_to_write}")
                    time.sleep(0.5)  # Small delay to avoid rate limiting
                
                logger.info("Successfully added new bonds to columns A and B.")
            else:
                logger.info("No new bonds found on the homepage.")

            # Small delay before fetching updated data
            time.sleep(2)

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
                
                # Add SUM formula at the bottom
                all_values_in_col_a = self.worksheet.col_values(1)  # Get all values from column A
                data_end_row = len(all_values_in_col_a)  # Last row with any data
                total_row_index = data_end_row + 2  # Skip one row, then add TOTAL
                
                sum_formula = f"=SUM({diff_col_letter}2:{diff_col_letter}{data_end_row})"
                self.worksheet.update_cell(total_row_index, diff_col_index, sum_formula)
                self.worksheet.update_cell(total_row_index, diff_col_index-1, "TOTAL:")
                logger.info(f"Added SUM formula to cell {diff_col_letter}{total_row_index}.")
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
