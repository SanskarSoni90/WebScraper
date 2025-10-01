import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import logging
import requests
import json
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple
import re

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
        """Extract timestamp from column header like 'Hourly Change (2025-10-01 12:01)'"""
        match = re.search(r'\((\d{4}-\d{2}-\d{2} \d{2}:\d{2})\)', header)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M").replace(tzinfo=self.ist_tz)
        return None

    def get_hourly_change_columns(self) -> List[Tuple[int, str, datetime]]:
        """Get all 'Hourly Change' columns with their indices, headers, and timestamps"""
        headers = self.worksheet.row_values(1)
        hourly_columns = []
        
        for idx, header in enumerate(headers, 1):
            if header and header.startswith("Hourly Change"):
                timestamp = self.parse_timestamp_from_header(header)
                if timestamp:
                    hourly_columns.append((idx, header, timestamp))
        
        return sorted(hourly_columns, key=lambda x: x[2])

    def calculate_volume(self, start_time: datetime, end_time: datetime) -> Dict[str, float]:
        """
        Calculate volume (sum of hourly changes) between two timestamps.
        Returns both raw price volume and face value-adjusted volume.
        """
        hourly_columns = self.get_hourly_change_columns()
        face_values = self.worksheet.col_values(3, value_render_option='UNFORMATTED_VALUE')[1:]  # Column C
        
        # Filter columns within the time range
        relevant_columns = [
            col for col in hourly_columns 
            if start_time <= col[2] <= end_time
        ]
        
        logger.info(f"Found {len(relevant_columns)} hourly change columns between {start_time} and {end_time}")
        
        raw_volume = 0
        adjusted_volume = 0
        column_count = 0
        
        for col_idx, col_header, col_time in relevant_columns:
            try:
                # Get values from this column (skip header)
                column_values = self.worksheet.col_values(col_idx, value_render_option='UNFORMATTED_VALUE')[1:]
                
                for row_idx, value in enumerate(column_values):
                    if value and value != '':
                        try:
                            # Parse the value (could be price difference)
                            price_diff = float(value)
                            
                            # Get face value for this bond
                            face_value = float(face_values[row_idx]) if row_idx < len(face_values) and face_values[row_idx] else 1
                            
                            # Raw volume is just the price difference
                            raw_volume += price_diff
                            
                            # Adjusted volume is price difference * face value
                            adjusted_volume += (price_diff * face_value)
                        except (ValueError, TypeError) as e:
                            continue
                
                column_count += 1
            except Exception as e:
                logger.error(f"Error processing column {col_header}: {e}")
                continue
        
        return {
            'raw_volume': raw_volume,
            'adjusted_volume': adjusted_volume,
            'column_count': column_count,
            'start_time': start_time,
            'end_time': end_time
        }

    def send_slack_alert(self, alert_type: str, volume_data: Dict):
        """Send formatted alert to Slack"""
        try:
            start_str = volume_data['start_time'].strftime("%Y-%m-%d %I:%M %p")
            end_str = volume_data['end_time'].strftime("%Y-%m-%d %I:%M %p")
            
            # Format numbers with commas and 2 decimal places
            raw_vol = volume_data['raw_volume']
            adj_vol = volume_data['adjusted_volume']
            
            # Create color based on values (green for positive, red for negative)
            color = "#36a64f" if adj_vol >= 0 else "#ff0000"
            
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
                                "title": "Raw Volume (Price Changes)",
                                "value": f"{raw_vol:,.2f}",
                                "short": True
                            },
                            {
                                "title": "Adjusted Volume (w/ Face Value)",
                                "value": f"₹{adj_vol:,.2f}",
                                "short": True
                            },
                            {
                                "title": "Data Points",
                                "value": f"{volume_data['column_count']} hourly snapshots",
                                "short": False
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
        """Alert 1: Last 24hrs volume (previous day 11am to today 11am)"""
        now = datetime.now(self.ist_tz)
        end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        # If current time is before 11:30 AM, use yesterday's 11 AM
        if now.hour < 11 or (now.hour == 11 and now.minute < 30):
            end_time = end_time - timedelta(days=1)
            start_time = start_time - timedelta(days=1)
        
        logger.info(f"Calculating 24hr volume (11am-11am): {start_time} to {end_time}")
        volume_data = self.calculate_volume(start_time, end_time)
        self.send_slack_alert("24hr Volume Report (11 AM - 11 AM)", volume_data)

    def send_24hr_6pm_alert(self):
        """Alert 2: Last 24hrs volume (previous day 6pm to today 6pm)"""
        now = datetime.now(self.ist_tz)
        end_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        # If current time is before 6:30 PM, use yesterday's 6 PM
        if now.hour < 18 or (now.hour == 18 and now.minute < 30):
            end_time = end_time - timedelta(days=1)
            start_time = start_time - timedelta(days=1)
        
        logger.info(f"Calculating 24hr volume (6pm-6pm): {start_time} to {end_time}")
        volume_data = self.calculate_volume(start_time, end_time)
        self.send_slack_alert("24hr Volume Report (6 PM - 6 PM)", volume_data)

    def send_mtd_alert(self):
        """Alert 3: MTD volume (1st day of month 11am to current 11am)"""
        now = datetime.now(self.ist_tz)
        
        # Start from 1st of current month at 11 AM
        start_time = now.replace(day=1, hour=11, minute=0, second=0, microsecond=0)
        
        # End at today's 11 AM (or yesterday's if before 11:30 AM today)
        end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
        if now.hour < 11 or (now.hour == 11 and now.minute < 30):
            end_time = end_time - timedelta(days=1)
        
        logger.info(f"Calculating MTD volume: {start_time} to {end_time}")
        volume_data = self.calculate_volume(start_time, end_time)
        self.send_slack_alert("Month-to-Date (MTD) Volume Report", volume_data)

    def run_scheduled_alerts(self):
        """Determine which alert to run based on current time"""
        now = datetime.now(self.ist_tz)
        current_hour = now.hour
        current_minute = now.minute
        
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %I:%M %p IST')}")
        
        # Alert 1 & 3: Run at 11:30 AM
        if current_hour == 11 and 30 <= current_minute < 35:
            logger.info("Running 11:30 AM alerts...")
            self.send_24hr_11am_alert()
            self.send_mtd_alert()
        
        # Alert 2: Run at 6:30 PM
        elif current_hour == 18 and 30 <= current_minute < 35:
            logger.info("Running 6:30 PM alert...")
            self.send_24hr_6pm_alert()
        
        else:
            logger.info(f"No scheduled alerts for current time: {current_hour}:{current_minute:02d}")

def main():
    """Main function to run alerts"""
    CREDENTIALS_PATH = 'service_account.json'
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
    
    try:
        alert_system = BondAlertSystem(CREDENTIALS_PATH, SPREADSHEET_URL, SLACK_WEBHOOK_URL)
        alert_system.run_scheduled_alerts()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'.")
    except Exception as e:
        logger.error(f"An error occurred during alert execution: {e}")

if __name__ == "__main__":
    main()
