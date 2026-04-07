#!/usr/bin/env python3
"""
Reads volunteer data from the '2025-2026-volunteers' workbook
(tab 'indoor_percussion_season'), filters to the next upcoming date,
and writes a sorted volunteer list to the 'percussion_print_outs' workbook
(tab 'volunteer_list') with green alternating row colors.
"""

import gspread
from google.oauth2.service_account import Credentials
import argparse
import os
import configparser
import datetime
import sys


def setup_google_sheets(credentials_file, scopes):
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return gspread.authorize(creds)


def main():
    parser = argparse.ArgumentParser(description="Create percussion volunteer list for the next event date.")
    parser.add_argument('--config-dir', type=str, default='.', help='Directory containing config.ini and credentials.json')
    args = parser.parse_args()

    config_dir = args.config_dir
    config_path = os.path.join(config_dir, 'config.ini')

    cp = configparser.ConfigParser()
    cp.read(config_path)
    if 'sheets' not in cp:
        print(f"Error: [sheets] section not found in {config_path}")
        sys.exit(1)
    config = dict(cp['sheets'])

    credentials_file = config.get('credentials', 'credentials.json')
    if not os.path.isabs(credentials_file):
        credentials_file = os.path.join(config_dir, credentials_file)
    scopes = config.get('scopes', 'https://www.googleapis.com/auth/drive').split(',')

    if not os.path.exists(credentials_file):
        print(f"Error: Credentials file {credentials_file} not found.")
        sys.exit(1)

    client = setup_google_sheets(credentials_file, scopes)

    # --- Read source data ---
    source_wb = client.open("2025-2026-volunteers")
    source_ws = source_wb.worksheet("indoor_percussion_season")
    all_data = source_ws.get_all_records()

    if not all_data:
        print("No data found in indoor_percussion_season.")
        return

    # --- Determine columns (case-insensitive lookup) ---
    # Build a mapping from lowercase key to actual key
    sample = all_data[0]
    key_map = {k.lower().strip(): k for k in sample.keys()}

    def col(name):
        """Return the actual column key matching the lowercase name."""
        for candidate in [name, name.replace('_', ''), name.replace(' ', '')]:
            if candidate in key_map:
                return key_map[candidate]
        # Try substring match
        for k, v in key_map.items():
            if name in k:
                return v
        return None

    date_col = col('startdate') or col('start_date') or col('start date')
    if date_col is None:
        # Print available columns so user can diagnose
        print(f"Could not find a start-date column. Available columns: {list(sample.keys())}")
        sys.exit(1)

    needed = {
        'location': col('location'),
        'item': col('item'),
        'firstname': col('firstname') or col('first_name') or col('first name'),
        'lastname': col('lastname') or col('last_name') or col('last name'),
        'phone': col('phone'),
        'email': col('email'),
    }
    missing = [k for k, v in needed.items() if v is None]
    if missing:
        print(f"Could not find columns: {missing}. Available: {list(sample.keys())}")
        sys.exit(1)

    # --- Parse dates and find the next date from today ---
    today = datetime.date.today()

    def parse_date(value):
        """Parse a date from a Google Sheets value (could be serial number or string)."""
        if isinstance(value, (int, float)):
            # Google Sheets serial number (days since 1899-12-30)
            epoch = datetime.date(1899, 12, 30)
            return epoch + datetime.timedelta(days=int(value))
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            # Try common formats
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y-%m-%dT%H:%M:%S',
                        '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M %p',
                        '%Y-%m-%d %H:%M:%S', '%m-%d-%Y'):
                try:
                    return datetime.datetime.strptime(value.split('T')[0].split(' ')[0], fmt.split(' ')[0].split('T')[0]).date()
                except ValueError:
                    continue
            # Last resort: dateutil
            try:
                from dateutil import parser as dp
                return dp.parse(value).date()
            except Exception:
                pass
        return None

    # Collect all future dates
    future_dates = set()
    for row in all_data:
        d = parse_date(row[date_col])
        if d and d > today:
            future_dates.add(d)

    if not future_dates:
        print("No future dates found in the data.")
        return

    next_date = min(future_dates)
    print(f"Next event date: {next_date}")

    # --- Filter rows for the next date ---
    filtered = []
    for row in all_data:
        d = parse_date(row[date_col])
        if d == next_date:
            filtered.append([
                str(row.get(needed['location'], '')),
                str(row.get(needed['item'], '')),
                str(row.get(needed['firstname'], '')),
                str(row.get(needed['lastname'], '')),
                str(row.get(needed['phone'], '')),
                str(row.get(needed['email'], '')),
            ])

    if not filtered:
        print(f"No volunteers found for {next_date}.")
        return

    # --- Sort by location, item, lastname ---
    filtered.sort(key=lambda r: (r[0].lower(), r[1].lower(), r[3].lower()))

    header = ['location', 'item', 'firstname', 'lastname', 'phone', 'email']
    data = [header] + filtered

    print(f"Found {len(filtered)} volunteer rows for {next_date}.")

    # --- Write to destination workbook ---
    dest_wb = client.open("percussion_print_outs")
    tab_name = "volunteer_list"
    try:
        dest_ws = dest_wb.worksheet(tab_name)
        dest_ws.clear()
        # Remove existing banding/filter views for this sheet (re-apply cleanly)
        wb_meta = dest_wb.fetch_sheet_metadata()
        sheet_meta = {}
        for sheet in wb_meta.get('sheets', []):
            props = sheet.get('properties', {})
            if props.get('sheetId') == dest_ws.id:
                sheet_meta = sheet
                break

        banded = sheet_meta.get('bandedRanges', [])
        if banded:
            dest_wb.batch_update({"requests": [
                {"deleteBanding": {"bandedRangeId": b['bandedRangeId']}} for b in banded
            ]})

        # Remove existing filter views
        filter_views = sheet_meta.get('filterViews', [])
        if filter_views:
            dest_wb.batch_update({"requests": [
                {"deleteFilterView": {"filterId": fv['filterViewId']}} for fv in filter_views
            ]})
    except gspread.exceptions.WorksheetNotFound:
        dest_ws = dest_wb.add_worksheet(title=tab_name, rows=max(len(data), 100), cols=len(header))

    dest_ws.update(values=data, range_name='A1')

    num_rows = len(data)
    num_cols = len(header)

    # --- Apply green alternating color banding ---
    requests = [
        {
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": dest_ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": num_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "rowProperties": {
                        "headerColor": {
                            "red": 0.22, "green": 0.46, "blue": 0.11, "alpha": 1
                        },
                        "firstBandColor": {
                            "red": 1, "green": 1, "blue": 1, "alpha": 1
                        },
                        "secondBandColor": {
                            "red": 0.85, "green": 0.92, "blue": 0.83, "alpha": 1
                        },
                    },
                }
            }
        },
        # Bold white header text
        {
            "repeatCell": {
                "range": {
                    "sheetId": dest_ws.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    }
                },
                "fields": "userEnteredFormat.textFormat",
            }
        },
        # Auto-resize columns
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": dest_ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_cols,
                }
            }
        },
    ]

    dest_wb.batch_update({"requests": requests})

    print(f"Wrote {len(filtered)} rows to 'percussion_print_outs' -> 'volunteer_list' for {next_date}.")


if __name__ == "__main__":
    main()
