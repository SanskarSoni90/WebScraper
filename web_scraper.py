import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import logging
import os
from typing import List, Dict, Optional
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebScraperGoogleSheets:
    def __init__(self, credentials_path: str, spreadsheet_url: str):
        """
        Initialize the scraper with Google Sheets credentials
        
        Args:
            credentials_path: Path to Google service account JSON file
            spreadsheet_url: URL of the Google Sheet
        """
        self.credentials_path = credentials_path
        self.spreadsheet_url = spreadsheet_url
        self.gc = None
        self.worksheet = None
        self.setup_google_sheets()
        
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
    
    def get_urls_from_sheet(self) -> List[str]:
        """Get all URLs from column B of the sheet"""
        try:
            # Get all values in column B (starting from row 2 to skip header)
            urls = self.worksheet.col_values(2)[1:]  # Column B, skip header
            # Filter out empty cells
            urls = [url.strip() for url in urls if url.strip()]
            logger.info(f"Retrieved {len(urls)} URLs from sheet")
            return urls
        except Exception as e:
            logger.error(f"Error getting URLs from sheet: {e}")
            return []
    
    def scrape_max_value(self, url: str) -> Optional[int]:
        """
        Scrape the max value from the specified input element
        
        Args:
            url: URL to scrape
            
        Returns:
            Max value as integer or None if not found
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for the specific input element
            input_element = soup.find('input', {
                'class': lambda x: x and 'unit-selector-input' in x,
                'type': 'number'
            })
            
            if input_element:
                max_value = input_element.get('max')
                if max_value:
                    return int(max_value)
                else:
                    logger.warning(f"No 'max' attribute found in input element for {url}")
                    return None
            else:
                logger.warning(f"Input element not found for {url}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
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
    
    def run_scraping_job(self):
        """Main method to run the complete scraping job"""
        logger.info("Starting scraping job...")
        
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
            max_value = self.scrape_max_value(url)
            max_values.append(max_value if max_value is not None else "")
            
            # Add delay to be respectful to servers
            time.sleep(2)
        
        # Update sheet with results
        self.update_sheet_with_values(max_values, timestamp)
        logger.info("Scraping job completed successfully")

def main():
    """Main function to run the scraper"""
    # Configuration
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        # Initialize scraper
        scraper = WebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL)
        
        # Run scraping job
        scraper.run_scraping_job()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
