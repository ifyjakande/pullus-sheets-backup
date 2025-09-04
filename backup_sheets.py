#!/usr/bin/env python3

import os
import json
import base64
import time
import random
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
import pytz
import pandas as pd
import boto3
from googleapiclient.discovery import build
from google.oauth2 import service_account
import requests
from functools import wraps
from config import SHEETS_CONFIG

def sanitize_error_message(error_msg: str) -> str:
    """Remove sensitive information from error messages"""
    # Remove sheet IDs (44-character alphanumeric strings)
    error_msg = re.sub(r'[a-zA-Z0-9_-]{44}', '[SHEET_ID]', error_msg)
    # Remove API URLs
    error_msg = re.sub(r'https://sheets\.googleapis\.com/[^\s]*', '[SHEETS_API_URL]', error_msg)
    error_msg = re.sub(r'https://[^\s]*googleapis\.com[^\s]*', '[GOOGLE_API_URL]', error_msg)
    return error_msg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RateLimiter:
    """Robust rate limiter with exponential backoff and jitter"""
    
    def __init__(self, max_retries: int = 5):
        self.max_retries = max_retries
        self.base_delay = 1.0
        self.max_delay = 60.0
    
    def exponential_backoff_with_jitter(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter"""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = delay * 0.25 * random.random()
        return delay + jitter
    
    def rate_limit_decorator(self, func):
        """Decorator for rate limiting API calls"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(self.max_retries):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logger.info(f"Success after {attempt + 1} attempts")
                    return result
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check for rate limit errors
                    if hasattr(e, 'resp') and hasattr(e.resp, 'status'):
                        status_code = e.resp.status
                        if status_code == 429:  # Rate limit exceeded
                            retry_after = getattr(e.resp, 'retry-after', None)
                            if retry_after:
                                delay = float(retry_after)
                                logger.warning(f"Rate limited. Waiting {delay}s (from Retry-After header)")
                            else:
                                delay = self.exponential_backoff_with_jitter(attempt)
                                logger.warning(f"Rate limited. Waiting {delay:.2f}s (exponential backoff)")
                        elif status_code >= 500:  # Server errors
                            delay = self.exponential_backoff_with_jitter(attempt)
                            logger.warning(f"Server error {status_code}. Retrying in {delay:.2f}s")
                        else:
                            logger.error(f"Non-retryable error {status_code}: {sanitize_error_message(str(e))}")
                            break
                    elif "quota" in str(e).lower() or "rate" in str(e).lower():
                        delay = self.exponential_backoff_with_jitter(attempt)
                        logger.warning(f"Quota/rate error. Retrying in {delay:.2f}s")
                    else:
                        logger.error(f"Non-retryable error: {sanitize_error_message(str(e))}")
                        break
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(delay)
                    else:
                        logger.error(f"Max retries ({self.max_retries}) exceeded")
            
            raise last_exception
        return wrapper

class SheetsBackup:
    """Main backup class with robust error handling and rate limiting"""
    
    def __init__(self):
        self.rate_limiter = RateLimiter()
        self.sheets_service = None
        self.s3_client = None
        self.bucket_name = os.getenv('AWS_S3_BUCKET')
        self.webhook_url = os.getenv('GOOGLE_CHAT_WEBHOOK')
        self.backup_results = []
        
        # Initialize services
        self._init_google_sheets()
        self._init_aws_s3()
    
    def _init_google_sheets(self):
        """Initialize Google Sheets API client"""
        try:
            service_account_json = base64.b64decode(
                os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            ).decode('utf-8')
            
            credentials_dict = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            logger.info("Google Sheets API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets API: {e}")
            raise
    
    def _init_aws_s3(self):
        """Initialize AWS S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_DEFAULT_REGION')
            )
            logger.info("AWS S3 client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize AWS S3 client: {e}")
            raise
    
    @RateLimiter().rate_limit_decorator
    def _fetch_sheet_data(self, sheet_id: str, sheet_name: str) -> pd.DataFrame:
        """Fetch data from a Google Sheet with rate limiting"""
        try:
            logger.info(f"Fetching data for sheet: {sheet_name}")
            
            # Get sheet metadata to find all sheets
            sheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            # Get data from all sheets in the spreadsheet
            all_data = []
            sheets = sheet_metadata.get('sheets', [])
            
            for sheet in sheets:
                sheet_title = sheet['properties']['title']
                range_name = f"'{sheet_title}'"
                
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=range_name,
                    valueRenderOption='FORMATTED_VALUE'
                ).execute()
                
                values = result.get('values', [])
                if values:
                    # Handle Pullus sheets structure: skip summary rows (0-1), use row 2 as header
                    if len(values) > 3:  # Need at least 4 rows (summary + header + data)
                        # Row 2 is the actual data header
                        header = values[2]
                        data_rows = []
                        
                        # Rows 3+ are actual data
                        for row in values[3:]:
                            # Pad or truncate row to match header length
                            if len(row) < len(header):
                                row.extend([''] * (len(header) - len(row)))
                            elif len(row) > len(header):
                                row = row[:len(header)]
                            data_rows.append(row)
                        
                        if data_rows:  # Only create DataFrame if we have data
                            df = pd.DataFrame(data_rows, columns=header)
                            # Add sheet name as a column
                            df['sheet_name'] = sheet_title
                            all_data.append(df)
                    elif len(values) > 1:
                        # Fallback for sheets with different structure
                        header = values[0]
                        data_rows = []
                        
                        for row in values[1:]:
                            if len(row) < len(header):
                                row.extend([''] * (len(header) - len(row)))
                            elif len(row) > len(header):
                                row = row[:len(header)]
                            data_rows.append(row)
                        
                        if data_rows:
                            df = pd.DataFrame(data_rows, columns=header)
                            df['sheet_name'] = sheet_title
                            all_data.append(df)
                
                # Add delay between sheet requests
                time.sleep(2)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"Successfully fetched {len(combined_df)} rows for {sheet_name}")
                return combined_df
            else:
                logger.warning(f"No data found for sheet: {sheet_name}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to fetch data for {sheet_name}: {sanitize_error_message(str(e))}")
            raise
    
    @RateLimiter().rate_limit_decorator
    def _upload_to_s3(self, data: pd.DataFrame, sheet_name: str) -> str:
        """Upload data to S3 in Parquet format with rate limiting"""
        try:
            # Use WAT timezone (UTC+1)
            wat_tz = pytz.timezone('Africa/Lagos')
            timestamp = datetime.now(wat_tz).strftime('%Y%m%d_%I-%M%p_WAT')
            s3_key = f"pullus/sales/{sheet_name}/{timestamp}.parquet"
            
            # Convert DataFrame to Parquet bytes
            parquet_buffer = data.to_parquet(index=False, engine='pyarrow')
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer,
                ContentType='application/octet-stream'
            )
            
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded {sheet_name} to {s3_url}")
            return s3_url
            
        except Exception as e:
            logger.error(f"Failed to upload {sheet_name} to S3: {e}")
            raise
    
    def backup_single_sheet(self, sheet_name: str, sheet_id: str) -> Dict[str, Any]:
        """Backup a single sheet with comprehensive error handling"""
        start_time = time.time()
        result = {
            'sheet_name': sheet_name,
            'success': False,
            'error': None,
            's3_url': None,
            'rows_backed_up': 0,
            'duration_seconds': 0
        }
        
        try:
            # Fetch sheet data
            data = self._fetch_sheet_data(sheet_id, sheet_name)
            
            if data.empty:
                result['error'] = "No data found in sheet"
                logger.warning(f"No data found for sheet: {sheet_name}")
                return result
            
            # Upload to S3
            s3_url = self._upload_to_s3(data, sheet_name)
            
            result.update({
                'success': True,
                's3_url': s3_url,
                'rows_backed_up': len(data),
                'duration_seconds': round(time.time() - start_time, 2)
            })
            
            logger.info(f"Successfully backed up {sheet_name}: {len(data)} rows in {result['duration_seconds']}s")
            
        except Exception as e:
            result.update({
                'error': sanitize_error_message(str(e)),
                'duration_seconds': round(time.time() - start_time, 2)
            })
            logger.error(f"Failed to backup {sheet_name}: {sanitize_error_message(str(e))}")
        
        return result
    
    def backup_all_sheets(self) -> List[Dict[str, Any]]:
        """Backup all configured sheets with proper spacing"""
        logger.info("Starting backup process for all sheets")
        start_time = time.time()
        
        for i, (sheet_name, sheet_id) in enumerate(SHEETS_CONFIG.items()):
            logger.info(f"Processing sheet {i+1}/{len(SHEETS_CONFIG)}: {sheet_name}")
            
            result = self.backup_single_sheet(sheet_name, sheet_id)
            self.backup_results.append(result)
            
            # Add delay between sheets to respect rate limits
            if i < len(SHEETS_CONFIG) - 1:  # Don't delay after the last sheet
                delay = 5 + random.uniform(1, 3)  # 5-8 seconds with jitter
                logger.info(f"Waiting {delay:.1f}s before next sheet...")
                time.sleep(delay)
        
        total_duration = time.time() - start_time
        logger.info(f"Backup process completed in {total_duration:.2f} seconds")
        
        return self.backup_results
    
    @RateLimiter().rate_limit_decorator
    def send_notification(self, results: List[Dict[str, Any]]):
        """Send notification to Google Chat with backup results"""
        try:
            successful = [r for r in results if r['success']]
            failed = [r for r in results if not r['success']]
            total_rows = sum(r['rows_backed_up'] for r in successful)
            
            # Use WAT timezone for notifications too
            wat_tz = pytz.timezone('Africa/Lagos')
            timestamp = datetime.now(wat_tz).strftime('%Y-%m-%d %I:%M:%S %p WAT')
            
            if failed:
                status = "⚠️ PARTIAL SUCCESS"
                color = "#FFA500"
            elif successful:
                status = "✅ SUCCESS"
                color = "#00FF00"
            else:
                status = "❌ FAILED"
                color = "#FF0000"
            
            message = {
                "cards": [{
                    "header": {
                        "title": f"Pullus Sales Backup - {status}",
                        "subtitle": timestamp
                    },
                    "sections": [{
                        "widgets": [{
                            "textParagraph": {
                                "text": f"<b>Summary:</b><br>"
                                       f"• Total sheets: {len(results)}<br>"
                                       f"• Successful: {len(successful)}<br>"
                                       f"• Failed: {len(failed)}<br>"
                                       f"• Total rows backed up: {total_rows:,}"
                            }
                        }]
                    }]
                }]
            }
            
            # Add failed sheets details if any
            if failed:
                failed_details = "\n".join([
                    f"• {r['sheet_name']}: {r['error']}" for r in failed
                ])
                message["cards"][0]["sections"].append({
                    "widgets": [{
                        "textParagraph": {
                            "text": f"<b>Failed Sheets:</b><br>{failed_details}"
                        }
                    }]
                })
            
            # Send notification
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            
            logger.info("Notification sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

def main():
    """Main execution function"""
    try:
        backup = SheetsBackup()
        results = backup.backup_all_sheets()
        backup.send_notification(results)
        
        # Print summary
        successful = len([r for r in results if r['success']])
        total = len(results)
        
        if successful == total:
            logger.info(f"✅ All {total} sheets backed up successfully")
            exit(0)
        elif successful > 0:
            logger.warning(f"⚠️ {successful}/{total} sheets backed up successfully")
            exit(1)
        else:
            logger.error(f"❌ All {total} sheets failed to backup")
            exit(2)
            
    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
        exit(3)

if __name__ == "__main__":
    main()