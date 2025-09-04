"""
Configuration file for Pullus Sheets Backup System

Contains configuration for backup operations.
All sheets will be backed up to S3 in Parquet format.
"""

import json
import os

def get_sheets_config():
    """Load sheets configuration from environment variable"""
    sheets_json = os.getenv('SHEETS_CONFIG_JSON')
    if not sheets_json:
        raise ValueError("SHEETS_CONFIG_JSON environment variable not found")
    
    try:
        return json.loads(sheets_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in SHEETS_CONFIG_JSON: {e}")

# Google Sheets configuration loaded from environment
SHEETS_CONFIG = get_sheets_config()

# S3 configuration
S3_PREFIX = 'pullus/sales'

# Backup settings
BACKUP_FORMAT = 'parquet'
RATE_LIMIT_DELAY = 2  # seconds between API calls
BATCH_DELAY = 5  # seconds between sheet processing