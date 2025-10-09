import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import logging
import requests
import json
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional
import re
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BondAlertSystem:
    def __init__(self, credentials_path: str, spreadsheet_url: str, slack_webhook_url: str):
        self.credentials_path = credentials_path
        self.spreadsheet_url = spreadsheet_url
        self.slack_webhook_url = slack_webhook_url
        self.gc = None
        self.worksheet = None
        self.ist_tz = ZoneInfo("Asia/Kolkata")
        self.setup_google_sheets()

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

    def parse_timestamp_from_header(self, header: str) -> datetime:
        """Extract timestamp from column header like 'Data (2025-10-03 15:50)'"""
        match = re.search(r'\((\d{4}-\d{2}-\d{2} \d{2}:\d{2})\)', header)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M").replace(tzinfo=self.ist_tz)
        return None

    def get_data_columns(self) -> List[Tuple[int, str, datetime]]:
        """Get all 'Data' columns with their indices, headers, and timestamps"""
        headers = self.worksheet.row_values(1)
        data_columns = []
        
        for idx, header in enumerate(headers, 1):
            if header and header.startswith("Data"):
                timestamp = self.parse_timestamp_from_header(header)
                if timestamp:
                    data_columns.append((idx, header, timestamp))
        
        return sorted(data_columns, key=lambda x: x[2])

    def find_closest_data_column(self, target_time: datetime, window_minutes: int = 60) -> Optional[Tuple[int, str, datetime]]:
        """
        Find the Data column closest to the target time within a time window.
        window_minutes: Look for data within ± this many minutes from target
        """
        data_columns = self.get_data_columns()
        
        closest_column = None
        min_time_diff = timedelta(days=999)
        
        for col_idx, col_header, col_time in data_columns:
            time_diff = abs(col_time - target_time)
            
            # Only consider columns within the window
            if time_diff <= timedelta(minutes=window_minutes) and time_diff < min_time_diff:
                min_time_diff = time_diff
                closest_column = (col_idx, col_header, col_time)
        
        if closest_column:
            logger.info(f"Found closest column to {target_time}: {closest_column[1]} (diff: {min_time_diff})")
        else:
            logger.warning(f"No data column found within {window_minutes} minutes of {target_time}")
        
        return closest_column

    def calculate_inventory_change(self, start_time: datetime, end_time: datetime) -> Dict:
        """
        Calculate net volume change between two time points.
        Returns the difference: (start_inventory - end_inventory) * face_value for all bonds.
        """
        # Find closest data columns to start and end times
        start_column = self.find_closest_data_column(start_time, window_minutes=60)
        end_column = self.find_closest_data_column(end_time, window_minutes=60)
        
        if not start_column or not end_column:
            logger.error("Could not find data columns for the specified time range")
            return {
                'net_change': 0,
                'start_time': start_time,
                'end_time': end_time,
                'start_snapshot': None,
                'end_snapshot': None,
                'error': 'Data columns not found'
            }
        
        start_col_idx, start_col_header, start_col_time = start_column
        end_col_idx, end_col_header, end_col_time = end_column
        
        # Get face value column (Column C, index 3)
        face_values = self.worksheet.col_values(3, value_render_option='UNFORMATTED_VALUE')[1:]
        
        # Get inventory values from both columns
        start_values = self.worksheet.col_values(start_col_idx, value_render_option='UNFORMATTED_VALUE')[1:]
        end_values = self.worksheet.col_values(end_col_idx, value_render_option='UNFORMATTED_VALUE')[1:]
        
        # Calculate net volume change across all bonds
        net_volume_change = 0
        bonds_processed = 0
        
        for row_idx in range(min(len(start_values), len(end_values), len(face_values))):
            try:
                start_inv = float(start_values[row_idx]) if start_values[row_idx] and start_values[row_idx] != '' else 0
                end_inv = float(end_values[row_idx]) if end_values[row_idx] and end_values[row_idx] != '' else 0
                face_value = float(face_values[row_idx]) if face_values[row_idx] and face_values[row_idx] != '' else 0
                
                # Quantity change = start - end (positive means inventory decreased/sold)
                quantity_change = start_inv - end_inv
                
                # Volume change = quantity change * face value
                volume_change = quantity_change * face_value
                
                net_volume_change += volume_change
                bonds_processed += 1
                
            except (ValueError, TypeError) as e:
                continue
        
        logger.info(f"Calculated volume change: ₹{net_volume_change:,.2f} across {bonds_processed} bonds")
        
        return {
            'net_change': net_volume_change,
            'start_time': start_col_time,
            'end_time': end_col_time,
            'start_snapshot': start_col_header,
            'end_snapshot': end_col_header,
            'bonds_processed': bonds_processed
        }

    def send_slack_alert(self, alert_type: str, change_data: Dict):
        """Send formatted alert to Slack"""
        try:
            if 'error' in change_data:
                logger.error(f"Cannot send alert due to error: {change_data['error']}")
                return
            
            start_str = change_data['start_time'].strftime("%Y-%m-%d %I:%M %p")
            end_str = change_data['end_time'].strftime("%Y-%m-%d %I:%M %p")
            
            net_change = change_data['net_change']
            
            # Create color based on net change (green for positive/sold, red for negative/bought)
            color = "#36a64f" if net_change >= 0 else "#ff0000"
            
            # Determine direction text
            if net_change > 0:
                direction = f"📉 Volume Decreased (Sold)"
            elif net_change < 0:
                direction = f"📈 Volume Increased (Bought)"
            else:
                direction = f"➡️ No Change"
            
            # Format the volume with Indian numbering system
            def format_indian_currency(amount):
                """Format currency in Indian style (Lakhs/Crores)"""
                abs_amount = abs(amount)
                sign = "-" if amount < 0 else ""
                
                if abs_amount >= 10000000:  # Crores
                    return f"{sign}₹{abs_amount/10000000:,.2f} Cr"
                elif abs_amount >= 100000:  # Lakhs
                    return f"{sign}₹{abs_amount/100000:,.2f} L"
                else:
                    return f"{sign}₹{abs_amount:,.2f}"
            
            formatted_change = format_indian_currency(net_change)
            
            message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"🔔 {alert_type}",
                        "fields": [
                            {
                                "title": "Time Period",
                                "value": f"{start_str}\n→ {end_str}",
                                "short": False
                            },
                            {
                                "title": "Net Volume Change",
                                "value": formatted_change,
                                "short": True
                            },
                            {
                                "title": "Direction",
                                "value": direction,
                                "short": True
                            },
                            {
                                "title": "Data Snapshots Used",
                                "value": f"Start: {change_data['start_snapshot']}\nEnd: {change_data['end_snapshot']}",
                                "short": False
                            },
                            {
                                "title": "Bonds Tracked",
                                "value": f"{change_data['bonds_processed']} bonds",
                                "short": True
                            }
                        ],
                        "footer": "Stablebonds Monitor",
                        "ts": int(datetime.now(self.ist_tz).timestamp())
                    }
                ]
            }
            
            response = requests.post(
                self.slack_webhook_url,
                data=json.dumps(message),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully sent {alert_type} to Slack")
            else:
                logger.error(f"Failed to send Slack alert. Status: {response.status_code}, Response: {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")

    def send_24hr_11am_alert(self):
        """Alert 1: 24hr volume change (yesterday ~11am to today ~11am)"""
        now = datetime.now(self.ist_tz)
        
        # Target times around 11 AM
        end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info(f"Calculating 24hr volume change (11am-11am)")
        change_data = self.calculate_inventory_change(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (11 AM - 11 AM)", change_data)

    def send_24hr_6pm_alert(self):
        """Alert 2: 24hr volume change (yesterday ~6pm to today ~6pm)"""
        now = datetime.now(self.ist_tz)
        
        # Target times around 6 PM
        end_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info(f"Calculating 24hr volume change (6pm-6pm)")
        change_data = self.calculate_inventory_change(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (6 PM - 6 PM)", change_data)

    def send_mtd_alert(self):
        """Alert 3: MTD volume change (1st of month ~11am to current time)"""
        now = datetime.now(self.ist_tz)
        current_hour = now.hour
        
        # Start from 1st of current month at 11 AM
        start_time = now.replace(day=1, hour=11, minute=0, second=0, microsecond=0)
        
        # End time depends on when this is called (11 AM or 6 PM)
        if 10 <= current_hour <= 11:
            end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
            alert_suffix = "11 AM"
        else:  # 6 PM window
            end_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
            alert_suffix = "6 PM"
        
        logger.info(f"Calculating MTD volume change: {start_time} to {end_time}")
        change_data = self.calculate_inventory_change(start_time, end_time)
        self.send_slack_alert(f"Month-to-Date Volume Change (as of {alert_suffix})", change_data)

    def run_scheduled_alerts(self):
        """Determine which alert to run based on current time - flexible timing"""
        now = datetime.now(self.ist_tz)
        current_hour = now.hour
        current_minute = now.minute
        
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %I:%M %p IST')}")
        
        # Alert 1 & 3: Run around 11 AM (10:45 AM to 11:45 AM window)
        if 10 <= current_hour <= 11:
            if (current_hour == 10 and current_minute >= 45) or (current_hour == 11 and current_minute <= 45):
                logger.info("Running 11 AM window alerts...")
                self.send_24hr_11am_alert()
                self.send_mtd_alert()
                return
        
        # Alert 2: Run around 6 PM (5:45 PM to 6:45 PM window)
        if 17 <= current_hour <= 18:
            if (current_hour == 17 and current_minute >= 45) or (current_hour == 18 and current_minute <= 45):
                logger.info("Running 6 PM window alert...")
                self.send_24hr_6pm_alert()
                self.send_mtd_alert()
                return
        
        logger.info(f"No scheduled alerts for current time: {current_hour}:{current_minute:02d}")

def main():
    """Main function to run alerts"""
    CREDENTIALS_PATH = 'service_account.json'
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    # Get Slack webhook URL from environment variable
    SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
    
    if not SLACK_WEBHOOK_URL:
        logger.error("SLACK_WEBHOOK_URL environment variable is not set!")
        logger.error("Please set it using: export SLACK_WEBHOOK_URL='your_webhook_url'")
        return
    
    try:
        alert_system = BondAlertSystem(CREDENTIALS_PATH, SPREADSHEET_URL, SLACK_WEBHOOK_URL)
        alert_system.run_scheduled_alerts()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'.")
    except Exception as e:
        logger.error(f"An error occurred during alert execution: {e}")

if __name__ == "__main__":
    main()
