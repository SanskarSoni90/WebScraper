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
import re

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
                        import re
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
    
    def analyze_page_structure(self, url: str):
        """
        Analyze the page structure to understand what elements are available
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            print(f"\n{'='*50}")
            print(f"ANALYZING PAGE: {url}")
            print(f"{'='*50}")
            
            # Find all input elements
            all_inputs = soup.find_all('input')
            print(f"\nFound {len(all_inputs)} input elements:")
            print("-" * 40)
            
            for i, inp in enumerate(all_inputs):
                print(f"Input {i+1}:")
                print(f"  Type: {inp.get('type', 'N/A')}")
                print(f"  Class: {inp.get('class', 'N/A')}")
                print(f"  Max: {inp.get('max', 'N/A')}")
                print(f"  Min: {inp.get('min', 'N/A')}")
                print(f"  Value: {inp.get('value', 'N/A')}")
                print(f"  Name: {inp.get('name', 'N/A')}")
                print(f"  ID: {inp.get('id', 'N/A')}")
                print(f"  Placeholder: {inp.get('placeholder', 'N/A')}")
                print(f"  Full element: {str(inp)[:100]}...")
                print()
            
            # Look for elements that might contain quantity/unit information
            print("\nLooking for quantity/unit related elements:")
            print("-" * 40)
            
            # Search for text patterns that might indicate quantity
            quantity_patterns = [
                r'quantity|unit|amount|qty|pieces|units',
                r'max.*quantity|maximum.*quantity',
                r'available|stock|inventory'
            ]
            
            for pattern in quantity_patterns:
                elements = soup.find_all(text=re.compile(pattern, re.IGNORECASE))
                if elements:
                    print(f"Found text matching '{pattern}': {len(elements)} occurrences")
                    for elem in elements[:3]:  # Show first 3 matches
                        print(f"  - {elem.strip()}")
            
            # Look for elements with specific attributes that might contain max values
            print("\nElements with 'max' attribute:")
            print("-" * 40)
            max_elements = soup.find_all(attrs={'max': True})
            for elem in max_elements:
                print(f"  {elem.name}: max='{elem.get('max')}', {str(elem)[:80]}...")
            
            # Look for select elements (dropdowns)
            selects = soup.find_all('select')
            if selects:
                print(f"\nFound {len(selects)} select elements:")
                print("-" * 40)
                for i, select in enumerate(selects):
                    options = select.find_all('option')
                    print(f"Select {i+1}: {len(options)} options")
                    print(f"  Class: {select.get('class', 'N/A')}")
                    print(f"  Name: {select.get('name', 'N/A')}")
                    print(f"  First few options: {[opt.get('value') for opt in options[:5]]}")
            
            # Look for data attributes that might contain quantity info
            print("\nElements with data-* attributes containing quantity info:")
            print("-" * 40)
            data_elements = soup.find_all(attrs={'data-max': True}) + \
                           soup.find_all(attrs={'data-quantity': True}) + \
                           soup.find_all(attrs={'data-stock': True})
            
            for elem in data_elements:
                print(f"  {elem.name}: {dict(elem.attrs)}")
            
            print(f"\n{'='*50}\n")
            
        except Exception as e:
            logger.error(f"Error analyzing page structure: {e}")
    
    def scrape_max_value(self, url: str, debug_mode: bool = False) -> Optional[int]:
        """
        Scrape the max value from the specified input element
        
        Args:
            url: URL to scrape
            debug_mode: If True, will print detailed debug information
            
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
            
            # Try the specific selector first based on your provided element
            target_element = None
            method_used = None
            
            # Method 1: Look for the exact element with both classes
            target_element = soup.find('input', {
                'class': lambda x: x and 'unit-selector-input' in str(x) and 'border-black-20' in str(x),
                'type': 'number'
            })
            if target_element:
                method_used = "exact class match (unit-selector-input + border-black-20)"
            
            # Method 2: Look for unit-selector-input class specifically
            if not target_element:
                target_element = soup.find('input', {
                    'class': lambda x: x and 'unit-selector-input' in str(x)
                })
                if target_element:
                    method_used = "unit-selector-input class"
            
            # Method 3: Look for the CSS selector path (simplified)
            if not target_element:
                # Try to find input in aside section
                aside = soup.find('aside')
                if aside:
                    target_element = aside.find('input', {'type': 'number'})
                    if target_element:
                        method_used = "aside input"
            
            # Method 4: Look for number input with inputmode="numeric"
            if not target_element:
                target_element = soup.find('input', {
                    'type': 'number',
                    'inputmode': 'numeric'
                })
                if target_element:
                    method_used = "numeric inputmode"
            
            # Method 5: Fallback to any number input with a min attribute
            if not target_element:
                number_inputs = soup.find_all('input', {'type': 'number'})
                for inp in number_inputs:
                    if inp.get('min') is not None:
                        target_element = inp
                        method_used = "fallback with min attr"
                        break
            
            if target_element:
                if debug_mode:
                    print(f"\nFound target element using: {method_used}")
                    print(f"Element: {target_element}")
                    print(f"Max attribute: {target_element.get('max')}")
                    print(f"Value attribute: {target_element.get('value')}")
                    print(f"Min attribute: {target_element.get('min')}")
                
                # Only get the max attribute - no fallbacks
                max_value = target_element.get('max')
                
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
                    if debug_mode:
                        print("All attributes:", dict(target_element.attrs))
                    return None
            else:
                logger.warning(f"Target input element not found for {url}")
                if debug_mode:
                    # Show all input elements for debugging
                    all_inputs = soup.find_all('input')
                    print(f"\nAll {len(all_inputs)} input elements found:")
                    for i, inp in enumerate(all_inputs):
                        print(f"  {i+1}: {inp}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Error converting max value to integer for {url}: {e}")
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
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
        
        # Get URLs from sheet
        urls = self.get_urls_from_sheet()
        if not urls:
            logger.error("No URLs found in sheet")
            return
        
        # If debug mode, analyze first URL structure
        if debug_mode and urls:
            print("Debug mode enabled - analyzing first URL structure...")
            self.analyze_page_structure(urls[0])
        
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

def debug_single_url():
    """Standalone function to debug a single URL in detail"""
    # Configuration
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        scraper = WebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL)
        test_url = "https://stablebonds.in/bonds/ugro-capital-limited/INE583D07570"
        
        # Analyze page structure first
        scraper.analyze_page_structure(test_url)
        
        # Try to scrape with debug mode
        max_val = scraper.scrape_max_value(test_url, debug_mode=True)
        print(f"\nFinal result: {max_val}")
        
    except Exception as e:
        logger.error(f"Error in debug: {e}")

def main():
    """Main function to run the scraper"""
    # Configuration
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    try:
        # Initialize scraper
        scraper = WebScraperGoogleSheets(CREDENTIALS_PATH, SPREADSHEET_URL)
        
        # For debugging, uncomment the line below
        # debug_single_url()
        # return
        
        # Run scraping job with debug mode for first few URLs
        scraper.run_scraping_job(debug_mode=True)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    # For immediate debugging, run this:
    debug_single_url()
    
    # For normal operation, run this instead:
    # main()
