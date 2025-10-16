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
            import traceback
            logger.error(traceback.format_exc())
            raise

    def parse_timestamp_from_header(self, header: str) -> Optional[datetime]:
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

    def find_closest_data_column(self, target_time: datetime, window_minutes: int = 30) -> Optional[Tuple[int, str, datetime]]:
        """
        Find the Data column closest to the target time within a time window.
        window_minutes: Look for data within ± this many minutes from target
        """
        data_columns = self.get_data_columns()
        
        closest_column = None
        min_time_diff = timedelta(days=999)
        
        for col_idx, col_header, col_time in data_columns:
            time_diff = abs(col_time - target_time)
            
            if time_diff <= timedelta(minutes=window_minutes) and time_diff < min_time_diff:
                min_time_diff = time_diff
                closest_column = (col_idx, col_header, col_time)
        
        if closest_column:
            logger.info(f"Found closest column to {target_time}: {closest_column[1]} (diff: {min_time_diff})")
        else:
            logger.warning(f"No data column found within {window_minutes} minutes of {target_time}")
        
        return closest_column

    def calculate_hourly_changes(self, start_time: datetime, end_time: datetime) -> Dict:
        """
        Calculate volume changes between consecutive hourly snapshots.
        Returns cumulative change and hourly breakdown.
        """
        # Generate list of target hourly timestamps
        hourly_timestamps = []
        current = start_time
        while current <= end_time:
            hourly_timestamps.append(current)
            current += timedelta(hours=1)
        
        logger.info(f"Calculating hourly changes from {start_time} to {end_time}")
        logger.info(f"Total hours to process: {len(hourly_timestamps) - 1}")
        
        # Fetch all sheet data once
        try:
            face_value_col_idx = 3
            all_data = self.worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
            all_data_rows = all_data[1:]  # Skip header
            face_values = [row[face_value_col_idx - 1] for row in all_data_rows]
        except Exception as e:
            logger.error(f"Failed to fetch batch data from sheet: {e}")
            return {'error': f'API data fetch failed: {e}'}
        
        cumulative_volume = 0
        bonds_processed = 0
        hourly_breakdown = []
        hours_processed = 0
        first_snapshot = None
        last_snapshot = None
        
        # Calculate changes between consecutive hours
        for i in range(len(hourly_timestamps) - 1):
            prev_target = hourly_timestamps[i]
            curr_target = hourly_timestamps[i + 1]
            
            prev_column = self.find_closest_data_column(prev_target, window_minutes=30)
            curr_column = self.find_closest_data_column(curr_target, window_minutes=30)
            
            if not prev_column or not curr_column:
                logger.warning(f"Skipping hour {prev_target.strftime('%I %p')} → {curr_target.strftime('%I %p')} - missing data")
                hourly_breakdown.append({
                    'prev_time': prev_target.strftime('%b %d %I %p'),
                    'curr_time': curr_target.strftime('%b %d %I %p'),
                    'change': None,
                    'missing': True
                })
                continue
            
            prev_col_idx, prev_col_header, prev_col_time = prev_column
            curr_col_idx, curr_col_header, curr_col_time = curr_column
            
            if first_snapshot is None:
                first_snapshot = prev_col_header
            last_snapshot = curr_col_header
            
            try:
                prev_values = [row[prev_col_idx - 1] for row in all_data_rows]
                curr_values = [row[curr_col_idx - 1] for row in all_data_rows]
            except IndexError:
                logger.warning(f"Skipping hour due to inconsistent sheet data")
                continue
            
            hour_volume = 0
            
            for row_idx in range(min(len(prev_values), len(curr_values), len(face_values))):
                try:
                    prev_inv = float(prev_values[row_idx]) if prev_values[row_idx] else 0
                    curr_inv = float(curr_values[row_idx]) if curr_values[row_idx] else 0
                    face_value = float(face_values[row_idx]) if face_values[row_idx] else 0
                    
                    quantity_change = prev_inv - curr_inv
                    volume_change = quantity_change * face_value
                    
                    hour_volume += volume_change
                    
                    if i == 0:
                        bonds_processed += 1
                        
                except (ValueError, TypeError):
                    continue
            
            cumulative_volume += hour_volume
            hours_processed += 1
            
            hourly_breakdown.append({
                'prev_time': prev_col_time.strftime('%b %d %I %p'),
                'curr_time': curr_col_time.strftime('%b %d %I %p'),
                'change': hour_volume,
                'missing': False
            })
            
            logger.info(f"Hour {i+1}: {prev_col_time.strftime('%I %p')} → {curr_col_time.strftime('%I %p')}: ₹{hour_volume:,.2f}")
        
        logger.info(f"Total cumulative volume: ₹{cumulative_volume:,.2f} across {hours_processed} hours")
        
        return {
            'net_change': cumulative_volume,
            'start_time': start_time,
            'end_time': end_time,
            'start_snapshot': first_snapshot,
            'end_snapshot': last_snapshot,
            'bonds_processed': bonds_processed,
            'hours_processed': hours_processed,
            'hourly_breakdown': hourly_breakdown
        }

    def calculate_mtd_volume_hourly(self, end_time: datetime) -> Dict:
        """
        Calculate MTD volume using hourly changes for each day, then sum across all days.
        """
        now = datetime.now(self.ist_tz)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        all_columns = self.get_data_columns()
        
        # Get all data columns within the current month
        month_columns = [
            (idx, header, ts) for idx, header, ts in all_columns
            if month_start <= ts <= end_time
        ]
        
        # Group snapshots by calendar day
        snapshots_by_day = {}
        for col_idx, header, ts in month_columns:
            day = ts.date()
            if day not in snapshots_by_day:
                snapshots_by_day[day] = []
            snapshots_by_day[day].append((col_idx, header, ts))
        
        if len(snapshots_by_day) < 1:
            logger.warning("No data available in current month for MTD calculation")
            return {
                'net_change': 0,
                'start_time': month_start,
                'end_time': end_time,
                'error': 'Insufficient data for MTD calculation',
                'days_processed': 0
            }
        
        sorted_days = sorted(snapshots_by_day.keys())
        logger.info(f"Calculating MTD volume across {len(sorted_days)} days with hourly granularity")
        
        # Fetch all sheet data once
        try:
            face_value_col_idx = 3
            all_data = self.worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
            all_data_rows = all_data[1:]
            face_values = [row[face_value_col_idx - 1] for row in all_data_rows]
        except Exception as e:
            logger.error(f"Failed to fetch batch data from sheet: {e}")
            return {'error': f'API data fetch failed: {e}'}
        
        cumulative_volume = 0
        bonds_processed = 0
        daily_breakdown = []
        first_snapshot = None
        last_snapshot = None
        total_hours_processed = 0
        
        # For each day, calculate hourly changes
        for day_idx, day in enumerate(sorted_days):
            day_snapshots = sorted(snapshots_by_day[day], key=lambda x: x[2])
            
            if len(day_snapshots) < 2:
                logger.info(f"Skipping {day} - not enough snapshots for hourly calculation")
                continue
            
            day_volume = 0
            day_hours = 0
            
            # Calculate changes between consecutive hours within this day
            for i in range(len(day_snapshots) - 1):
                prev_col_idx, prev_col_header, prev_col_time = day_snapshots[i]
                curr_col_idx, curr_col_header, curr_col_time = day_snapshots[i + 1]
                
                if first_snapshot is None:
                    first_snapshot = prev_col_header
                last_snapshot = curr_col_header
                
                try:
                    prev_values = [row[prev_col_idx - 1] for row in all_data_rows]
                    curr_values = [row[curr_col_idx - 1] for row in all_data_rows]
                except IndexError:
                    logger.warning(f"Skipping interval due to inconsistent sheet data")
                    continue
                
                interval_volume = 0
                
                for row_idx in range(min(len(prev_values), len(curr_values), len(face_values))):
                    try:
                        prev_inv = float(prev_values[row_idx]) if prev_values[row_idx] else 0
                        curr_inv = float(curr_values[row_idx]) if curr_values[row_idx] else 0
                        face_value = float(face_values[row_idx]) if face_values[row_idx] else 0
                        
                        quantity_change = prev_inv - curr_inv
                        volume_change = quantity_change * face_value
                        
                        interval_volume += volume_change
                        
                        if day_idx == 0 and i == 0:
                            bonds_processed += 1
                            
                    except (ValueError, TypeError):
                        continue
                
                day_volume += interval_volume
                day_hours += 1
                total_hours_processed += 1
            
            cumulative_volume += day_volume
            
            daily_breakdown.append({
                'date': day.strftime('%b %d'),
                'change': day_volume,
                'hours': day_hours
            })
            
            logger.info(f"Day {day.strftime('%b %d')}: ₹{day_volume:,.2f} across {day_hours} intervals")
        
        logger.info(f"Total MTD cumulative volume: ₹{cumulative_volume:,.2f} across {len(daily_breakdown)} days and {total_hours_processed} hourly intervals")
        
        return {
            'net_change': cumulative_volume,
            'start_time': month_start,
            'end_time': end_time,
            'start_snapshot': first_snapshot,
            'end_snapshot': last_snapshot,
            'bonds_processed': bonds_processed,
            'days_processed': len(daily_breakdown),
            'hours_processed': total_hours_processed,
            'daily_breakdown': daily_breakdown
        }

    def format_indian_currency(self, amount):
        """Format currency in Indian style (Lakhs/Crores)"""
        abs_amount = abs(amount)
        sign = "-" if amount < 0 else ""
        
        if abs_amount >= 10000000:  # Crores
            return f"{sign}₹{abs_amount/10000000:,.2f} Cr"
        elif abs_amount >= 100000:  # Lakhs
            return f"{sign}₹{abs_amount/100000:,.2f} L"
        else:
            return f"{sign}₹{abs_amount:,.2f}"

    def send_slack_alert(self, alert_type: str, change_data: Dict, is_mtd: bool = False):
        """Send formatted alert to Slack"""
        try:
            if 'error' in change_data:
                logger.error(f"Cannot send alert due to error: {change_data['error']}")
                return
            
            start_str = change_data['start_time'].strftime("%Y-%m-%d %I:%M %p")
            end_str = change_data['end_time'].strftime("%Y-%m-%d %I:%M %p")
            
            net_change = change_data['net_change']
            
            color = "#36a64f" if net_change >= 0 else "#ff0000"
            
            if net_change > 0:
                direction = "📉 Volume Decreased (Sold)"
            elif net_change < 0:
                direction = "📈 Volume Increased (Bought)"
            else:
                direction = "➡️ No Change"
            
            formatted_change = self.format_indian_currency(net_change)
            
            fields = [
                {
                    "title": "Time Period",
                    "value": f"{start_str}\n→ {end_str}",
                    "short": False
                },
                {
                    "title": "Net Volume Change" if not is_mtd else "Cumulative MTD Volume",
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
            ]
            
            if is_mtd and 'days_processed' in change_data:
                fields.append({
                    "title": "Calculation Method",
                    "value": f"Cumulative across {change_data['days_processed']} days using {change_data['hours_processed']} hourly intervals",
                    "short": False
                })
            elif 'hours_processed' in change_data:
                fields.append({
                    "title": "Calculation Method",
                    "value": f"Cumulative across {change_data['hours_processed']} hourly intervals",
                    "short": False
                })
            
            # Add hourly breakdown for 24hr alerts
            if not is_mtd and 'hourly_breakdown' in change_data and change_data['hourly_breakdown']:
                breakdown_lines = []
                for hour_data in change_data['hourly_breakdown']:
                    if hour_data.get('missing'):
                        breakdown_lines.append(f"{hour_data['prev_time']} → {hour_data['curr_time']}: ⚠️ Missing data")
                    else:
                        formatted_amount = self.format_indian_currency(hour_data['change'])
                        breakdown_lines.append(f"{hour_data['prev_time']} → {hour_data['curr_time']}: {formatted_amount}")
                
                breakdown_text = "\n".join(breakdown_lines)
                fields.append({
                    "title": "⏱️ Hourly Breakdown",
                    "value": breakdown_text,
                    "short": False
                })
            
            # Add daily breakdown for MTD alerts
            if is_mtd and 'daily_breakdown' in change_data and change_data['daily_breakdown']:
                breakdown_lines = []
                for day_data in change_data['daily_breakdown']:
                    formatted_amount = self.format_indian_currency(day_data['change'])
                    breakdown_lines.append(f"{day_data['date']}: {formatted_amount} ({day_data['hours']} intervals)")
                
                breakdown_text = "\n".join(breakdown_lines)
                fields.append({
                    "title": "📊 Daily Breakdown",
                    "value": breakdown_text,
                    "short": False
                })
            
            message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"🔔 {alert_type}",
                        "fields": fields,
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
        """Alert 1: 24hr volume change with hourly breakdown (yesterday 11am to today 11am)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info("Calculating 24hr volume change with hourly breakdown (11am-11am)")
        change_data = self.calculate_hourly_changes(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (11 AM - 11 AM)", change_data)

    def send_24hr_6pm_alert(self):
        """Alert 2: 24hr volume change with hourly breakdown (yesterday 6pm to today 6pm)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info("Calculating 24hr volume change with hourly breakdown (6pm-6pm)")
        change_data = self.calculate_hourly_changes(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (6 PM - 6 PM)", change_data)

    def send_mtd_alert(self):
        """Alert 3: MTD cumulative volume change with daily breakdown (hourly granularity)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=23, minute=59, second=59)
        alert_suffix = now.strftime("%I %p")

        logger.info(f"Calculating MTD cumulative volume with hourly granularity up to {end_time}")
        change_data = self.calculate_mtd_volume_hourly(end_time)
        self.send_slack_alert(f"Month-to-Date Cumulative Volume (as of {alert_suffix})", change_data, is_mtd=True)

    def run_scheduled_alerts(self):
        """Determine which alert to run based on current time"""
        now = datetime.now(self.ist_tz)
        current_hour = now.hour
        current_minute = now.minute
        
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %I:%M %p IST')}")
        
        # 11 AM window: 10:30 AM to 11:30 AM
        if (current_hour == 10 and current_minute >= 30) or (current_hour == 11 and current_minute <= 30):
            logger.info("Running 11 AM window alerts...")
            self.send_24hr_11am_alert()
            self.send_mtd_alert()
            return
        
        # 6 PM window: 5:30 PM to 6:30 PM
        if (current_hour == 17 and current_minute >= 30) or (current_hour == 18 and current_minute <= 30):
            logger.info("Running 6 PM window alert...")
            self.send_24hr_6pm_alert()
            return

        # 9 PM window: 21:10 to 21:40
        if (current_hour == 21 and current_minute >= 10) or (current_hour == 21 and current_minute <= 40):
            logger.info("Running 9 PM window alerts...")
            self.send_24hr_6pm_alert()
            self.send_mtd_alert()
            return
        
        logger.info(f"No scheduled alerts for current time: {now.strftime('%I:%M %p')}")

def main():
    """Main function to run alerts"""
    CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS', 'service_account.json')
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1dIFvqToTTF0G9qyRy6dSdAtVOU763K0N3iOLkp0iWJY/edit?gid=0#gid=0'
    
    SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
    
    if not CREDENTIALS_PATH:
        logger.error("GOOGLE_CREDENTIALS environment variable is not set and no default found!")
        return
    
    if not SLACK_WEBHOOK_URL:
        logger.error("SLACK_WEBHOOK_URL environment variable is not set!")
        return
    
    try:
        alert_system = BondAlertSystem(CREDENTIALS_PATH, SPREADSHEET_URL, SLACK_WEBHOOK_URL)
        alert_system.run_scheduled_alerts()
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found at '{CREDENTIALS_PATH}'.")
    except Exception as e:
        logger.error(f"An error occurred during alert execution: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
