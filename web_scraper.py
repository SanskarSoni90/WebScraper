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
# --- NEW IMPORTS ---
from gspread.utils import a1_to_rowcol, rowcol_to_a1
from gspread.models import ConditionalFormatRule, BooleanCondition, CellFormat

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumWebScraperGoogleSheets:
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
    
    def get_urls_from_sheet(self) -> List[Dict[str, any]]:
        """Get all URLs and their corresponding row numbers from column B using an efficient batch request."""
        try:
            logger.info("Fetching all URLs from sheet in a single batch request...")
            all_formulas = self.worksheet.get('B2:B', value_render_option='FORMULA')
            
            url_data = []
            for index, formula_cell in enumerate(all_formulas):
                row_num = index + 2
                if not formula_cell:
                    continue
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
        """Scrape the max value from the specified input element using Selenium."""
        try:
            self.driver.get(url)
            time.sleep(3)
            
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
                        element = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector)))
                    else:
                        element = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    target_element = element
                    break
                except TimeoutException:
                    continue
            
            if target_element:
                max_value = target_element.get_attribute('max')
                if max_value is not None:
                    try:
                        return int(max_value)
                    except (ValueError, TypeError):
                        return None
                else:
                    return None
            else:
                return None
        except Exception:
            return None
    
    def run_scraping_job(self):
        """Main method to run the complete scraping and updating job."""
        logger.info("Starting scraping job...")
        
        try:
            # === PART 1: SCRAPE AND WRITE DATA ===
            url_infos = self.get_urls_from_sheet()
            if not url_infos:
                logger.error("No URLs found in the sheet. Exiting.")
                return

            header_row = self.worksheet.row_values(1)
            # Find the first column that doesn't start with "Hourly Change"
            raw_data_columns = [h for h in header_row if not str(h).startswith("Hourly Change")]
            next_data_col_index = len(raw_data_columns) + 1

            ist_tz = ZoneInfo("Asia/Kolkata")
            timestamp_str = datetime.now(ist_tz).strftime("%Y-%m-%d %H:%M")
            header_title = f"Data ({timestamp_str})"
            self.worksheet.update_cell(1, next_data_col_index, header_title)
            
            for i, url_info in enumerate(url_infos, 1):
                url, row_num = url_info['url'], url_info['row']
                logger.info(f"Processing {i}/{len(url_infos)}: Row {row_num}, URL {url}")
                max_value = self.scrape_max_value(url)
                value_to_write = max_value if max_value is not None else ""
                self.worksheet.update_cell(row_num, next_data_col_index, value_to_write)
                time.sleep(1) # Reduced sleep time
            
            # === PART 2: AUTOMATED ANALYSIS ===
            # This part runs only if there are at least two data columns to compare
            # Assuming data starts in column C (index 3), so we need C and D.
            if next_data_col_index > 3:
                logger.info("Performing automated analysis...")

                # 1. Define columns for calculation
                prev_data_col = next_data_col_index - 1
                curr_data_col = next_data_col_index
                
                # The new difference column will be the next available column in the sheet
                all_headers = self.worksheet.row_values(1)
                diff_col_index = len(all_headers) + 1
                diff_col_letter = rowcol_to_a1(1, diff_col_index)[:-1]

                # 2. Read the two most recent data columns
                prev_values_str = self.worksheet.col_values(prev_data_col, value_render_option='UNFORMATTED_VALUE')[1:]
                curr_values_str = self.worksheet.col_values(curr_data_col, value_render_option='UNFORMATTED_VALUE')[1:]

                # 3. Calculate the differences
                diff_values = []
                for prev, curr in zip(prev_values_str, curr_values_str):
                    try:
                        # Convert to numbers, calculate difference. Default to 0 if empty.
                        diff = float(curr or 0) - float(prev or 0)
                        diff_values.append([diff])
                    except (ValueError, TypeError):
                        diff_values.append([""]) # Leave blank if values are not numbers

                # 4. Write the new "Hourly Change" column
                diff_header = f"Hourly Change ({timestamp_str})"
                self.worksheet.update_cell(1, diff_col_index, diff_header)
                if diff_values:
                    self.worksheet.update(f'{diff_col_letter}2', diff_values)
                logger.info(f"Added '{diff_header}' column at index {diff_col_index}.")
                
                # 5. Add SUM formula at the bottom
                # Assuming max 1000 rows of data.
                total_row_index = len(url_infos) + 3 
                sum_formula = f"=SUM({diff_col_letter}2:{diff_col_letter}{total_row_index-1})"
                self.worksheet.update_cell(total_row_index, diff_col_index, sum_formula)
                self.worksheet.update_cell(total_row_index, diff_col_index-1, "TOTAL:")
                logger.info(f"Added SUM formula to cell {diff_col_letter}{total_row_index}.")
                
                # 6. Add conditional formatting to highlight negative numbers
                rule = ConditionalFormatRule(
                    ranges=[f'{diff_col_letter}2:{diff_col_letter}{total_row_index-1}'],
                    booleanRule=BooleanCondition(
                        'NUMBER_LESS_THAN',
                        ['0']
                    ),
                    format=CellFormat(backgroundColor={'red': 0.9, 'green': 0.6, 'blue': 0.6}) # Light red
                )
                self.worksheet.add_conditional_format_rule(rule)
                logger.info(f"Added conditional formatting to column {diff_col_letter}.")

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
        scraper = SeleniumWebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL, headless=True)
        scraper.run_scraping_job()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'.")
    except Exception as e:
        logger.error(f"An error occurred during the main execution: {e}")

if __name__ == "__main__":
    main()
