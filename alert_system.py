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
        
        try:
            face_value_col_idx = 3
            all_data = self.worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
            
            all_data_rows = all_data[1:]
            face_values = [row[face_value_col_idx - 1] for row in all_data_rows]
            start_values = [row[start_col_idx - 1] for row in all_data_rows]
            end_values = [row[end_col_idx - 1] for row in all_data_rows]
            
        except IndexError:
            logger.error("A column index was out of range. Check sheet structure.")
            return {'error': 'Sheet data structure is inconsistent.'}
        except Exception as e:
            logger.error(f"Failed to fetch batch data from sheet: {e}")
            return {'error': f'API data fetch failed: {e}'}

        net_volume_change = 0
        bonds_processed = 0
        
        for row_idx in range(min(len(start_values), len(end_values), len(face_values))):
            try:
                start_inv = float(start_values[row_idx]) if start_values[row_idx] else 0
                end_inv = float(end_values[row_idx]) if end_values[row_idx] else 0
                face_value = float(face_values[row_idx]) if face_values[row_idx] else 0
                
                quantity_change = start_inv - end_inv
                volume_change = quantity_change * face_value
                
                net_volume_change += volume_change
                bonds_processed += 1
                
            except (ValueError, TypeError):
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

    def calculate_mtd_volume(self, end_time: datetime) -> Dict:
        """
        Calculate cumulative MTD volume by summing the changes between the ~11 AM snapshot of each day.
        """
        now = datetime.now(self.ist_tz)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        all_columns = self.get_data_columns()
        
        # 1. Get all data columns within the current month
        month_columns = [
            (idx, header, ts) for idx, header, ts in all_columns
            if month_start <= ts <= end_time
        ]

        # 2. Group these snapshots by their calendar day
        snapshots_by_day = {}
        for col_idx, header, ts in month_columns:
            day = ts.date()
            if day not in snapshots_by_day:
                snapshots_by_day[day] = []
            snapshots_by_day[day].append((col_idx, header, ts))

        # 3. For each day, find the single snapshot closest to 11:00 AM
        daily_11am_columns = []
        for day, snapshots in snapshots_by_day.items():
            target_11am = datetime(day.year, day.month, day.day, 11, 0, tzinfo=self.ist_tz)
            closest_snapshot = min(snapshots, key=lambda s: abs(s[2] - target_11am))
            daily_11am_columns.append(closest_snapshot)
        
        # 4. Sort the chosen daily snapshots chronologically
        daily_11am_columns.sort(key=lambda x: x[2])
        
        if len(daily_11am_columns) < 2:
            logger.warning("Not enough daily 11 AM snapshots in current month for MTD calculation")
            return {
                'net_change': 0,
                'start_time': month_start,
                'end_time': end_time,
                'error': 'Insufficient data for MTD calculation (need at least two days with ~11 AM data)',
                'snapshots_used': len(daily_11am_columns)
            }
        
        logger.info(f"Calculating MTD volume using {len(daily_11am_columns)} daily 11 AM snapshots.")

        try:
            all_data = self.worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
            all_data_rows = all_data[1:] # Skip header row
            face_values = [row[2] for row in all_data_rows] # Face value is in column C (index 2)
        except Exception as e:
            logger.error(f"Failed to fetch batch data from sheet: {e}")
            return {'error': f'API data fetch failed: {e}'}

        cumulative_volume = 0
        bonds_processed = 0
        snapshots_processed = 0
        
        # 5. Calculate cumulative changes between consecutive daily 11 AM snapshots
        for i in range(len(daily_11am_columns) - 1):
            prev_col_idx, _, prev_time = daily_11am_columns[i]
            curr_col_idx, _, curr_time = daily_11am_columns[i + 1]
            
            try:
                prev_values = [row[prev_col_idx - 1] for row in all_data_rows]
                curr_values = [row[curr_col_idx - 1] for row in all_data_rows]
            except IndexError:
                logger.warning(f"Skipping interval due to inconsistent sheet data for columns {prev_col_idx} or {curr_col_idx}")
                continue

            snapshot_volume = 0
            
            for row_idx in range(min(len(prev_values), len(curr_values), len(face_values))):
                try:
                    prev_inv = float(prev_values[row_idx]) if prev_values[row_idx] else 0
                    curr_inv = float(curr_values[row_idx]) if curr_values[row_idx] else 0
                    face_value = float(face_values[row_idx]) if face_values[row_idx] else 0
                    
                    quantity_change = prev_inv - curr_inv
                    volume_change = quantity_change * face_value
                    
                    snapshot_volume += volume_change
                    
                    if i == 0:
                        bonds_processed += 1
                        
                except (ValueError, TypeError):
                    continue
            
            cumulative_volume += snapshot_volume
            snapshots_processed += 1
            logger.info(f"Day-to-Day Change {i+1}: {prev_time.strftime('%b %d')} → {curr_time.strftime('%b %d')}: ₹{snapshot_volume:,.2f}")
        
        logger.info(f"Total MTD cumulative volume: ₹{cumulative_volume:,.2f} across {snapshots_processed} daily intervals")
        
        return {
            'net_change': cumulative_volume,
            'start_time': daily_11am_columns[0][2],
            'end_time': daily_11am_columns[-1][2],
            'start_snapshot': daily_11am_columns[0][1],
            'end_snapshot': daily_11am_columns[-1][1],
            'bonds_processed': bonds_processed,
            'snapshots_used': len(daily_11am_columns),
            'intervals_calculated': snapshots_processed
        }


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
            
            if is_mtd and 'snapshots_used' in change_data:
                fields.append({
                    "title": "Calculation Method",
                    "value": f"Cumulative across {change_data['intervals_calculated']} daily intervals using {change_data['snapshots_used']} snapshots",
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
        """Alert 1: 24hr volume change (yesterday ~11am to today ~11am)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=11, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info("Calculating 24hr volume change (11am-11am)")
        change_data = self.calculate_inventory_change(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (11 AM - 11 AM)", change_data)

    def send_24hr_6pm_alert(self):
        """Alert 2: 24hr volume change (yesterday ~6pm to today ~6pm)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        
        logger.info("Calculating 24hr volume change (6pm-6pm)")
        change_data = self.calculate_inventory_change(start_time, end_time)
        self.send_slack_alert("24hr Volume Change (6 PM - 6 PM)", change_data)

    def send_mtd_alert(self):
        """Alert 3: MTD cumulative volume change (all daily changes from 1st of month to now)"""
        now = datetime.now(self.ist_tz)
        
        end_time = now.replace(hour=23, minute=59, second=59) # Ensure we get all of today's snapshots
        alert_suffix = now.strftime("%I %p")

        logger.info(f"Calculating MTD cumulative volume up to {end_time}")
        change_data = self.calculate_mtd_volume(end_time)
        self.send_slack_alert(f"Month-to-Date Cumulative Volume (as of {alert_suffix})", change_data, is_mtd=True)

    def run_scheduled_alerts(self):
        """Determine which alert to run based on current time - flexible timing"""
        now = datetime.now(self.ist_tz)
        current_hour = now.hour
        current_minute = now.minute
        
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %I:%M %p IST')}")
        
        # Window for 11 AM alerts
        if current_hour == 11:
            logger.info("Running 11 AM window alerts...")
            self.send_24hr_11am_alert()
            self.send_mtd_alert()
            return
        
        # Window for 6 PM alerts
        if current_hour == 18:
            logger.info("Running 6 PM window alert...")
            self.send_24hr_6pm_alert()
            self.send_mtd_alert()
            return
        
        # Window for 7:10 PM test alerts (runs between 7:05 PM and 7:15 PM)
        if current_hour == 19 and 5 <= current_minute <= 15:
            logger.info("Running 7:10 PM test alerts...")
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
