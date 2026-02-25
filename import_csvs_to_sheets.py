#!/usr/bin/env python3
"""
Script to import CSV files from /home/ruddy/data/agimba/2025 to Google Sheets.

Creates a workbook named "2025-2026-volunteers" if it doesn't exist,
and creates a sheet for each CSV, importing the data as a table.

Copyright (C) 2026  Robert Ruddy


This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import gspread
from google.oauth2.service_account import Credentials
import csv
import sys
import os
import glob
import configparser
import time
import re

def setup_google_sheets(credentials_file, scopes):
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def get_or_create_workbook(client, name):
    # Debug: list accessible spreadsheets
    try:
        spreadsheets = client.list_spreadsheet_files()
        print("Accessible spreadsheets:")
        for s in spreadsheets:
            print(f"  - {s['name']}")
    except Exception as e:
        print(f"Error listing spreadsheets: {e}")
    
    try:
        return client.open(name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Workbook '{name}' not found. Attempting to create...")
        return client.create(name)
    except Exception as e:
        print(f"Error accessing workbook '{name}': {e}")
        sys.exit(1)

def build_email_phone_map(csv_dir, exclude_columns):
    email_phone_map = {}
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    for csv_file in csv_files:
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get('email', '').strip().lower()
                phone = row.get('phone', '').strip()
                digits = re.sub(r'\D', '', phone)
                if email and len(digits) == 10:
                    # Always prefer a valid phone if not already set
                    if email not in email_phone_map:
                        email_phone_map[email] = f"{digits[0:3]}.{digits[3:6]}.{digits[6:10]}"
    return email_phone_map

def normalize_phone(phone):
    digits = re.sub(r'\D', '', phone or '')
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}.{digits[3:6]}.{digits[6:10]}"
    return None

def qa_phone_numbers(rows, header):
    # Find the phone and email column indices
    phone_idx = next((i for i, h in enumerate(header) if 'phone' in h.lower()), None)
    email_idx = next((i for i, h in enumerate(header) if 'email' in h.lower()), None)
    if phone_idx is None or email_idx is None:
        return rows  # Nothing to do

    # First pass: collect valid phone numbers by email
    email_to_phone = {}
    for row in rows:
        email = row[email_idx].strip().lower()
        phone = normalize_phone(row[phone_idx])
        if phone:
            email_to_phone[email] = phone

    # Second pass: fix or discard invalid phone numbers, fill in missing ones
    fixed_rows = []
    for row in rows:
        email = row[email_idx].strip().lower()
        phone = normalize_phone(row[phone_idx])
        if not phone:
            # Try to fill from known valid phones
            phone = email_to_phone.get(email, '')
        row[phone_idx] = phone
        fixed_rows.append(row)
    return fixed_rows

def qa_phone_numbers_with_global_map(rows, header, global_email_phone_map):
    phone_idx = next((i for i, h in enumerate(header) if 'phone' in h.lower()), None)
    email_idx = next((i for i, h in enumerate(header) if 'email' in h.lower()), None)
    if phone_idx is None or email_idx is None:
        return rows
    fixed_rows = []
    for row in rows:
        email = row[email_idx].strip().lower()
        phone = normalize_phone(row[phone_idx])
        if not phone:
            phone = global_email_phone_map.get(email, '')
        row[phone_idx] = phone
        fixed_rows.append(row)
    return fixed_rows

def import_csv_to_sheet(workbook, csv_file, global_email_phone_map):
    sheet_name = os.path.splitext(os.path.basename(csv_file))[0]
    try:
        worksheet = workbook.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = workbook.add_worksheet(title=sheet_name, rows=1000, cols=26)
    
    # Columns to exclude
    exclude_columns = {'amountpaid', 'slotitemid', 'hastime', 'status', 'starttime', 'startdate', 'phonetype', 'offset', 'endtime', 'itemmemberid', 'signupid', 'signedupdate', 'enddate', 'waitlist'}
    
    import datetime
    from dateutil import parser as date_parser
    import pytz
    with open(csv_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        filtered_data = []
        for row in reader:
            filtered_row = {k: v for k, v in row.items() if k not in exclude_columns}
            for key in list(filtered_row.keys()):
                # Remove 'string' from field names
                if 'string' in key:
                    new_key = key.replace('string', '').replace('__', '_').strip('_')
                    filtered_row[new_key] = filtered_row.pop(key)
            for key in filtered_row:
                if 'phone' in key.lower():
                    filtered_row[key] = normalize_phone(filtered_row[key])
                # Format date fields as Google Sheets serial numbers (convert UTC to US/Eastern)
                if 'date' in key.lower() and filtered_row[key]:
                    try:
                        dt = date_parser.parse(filtered_row[key])
                        # If tz-aware, convert to UTC first
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(pytz.utc)
                        else:
                            dt = pytz.utc.localize(dt)
                        # Convert to US/Eastern
                        eastern = pytz.timezone('US/Eastern')
                        dt = dt.astimezone(eastern).replace(tzinfo=None)
                        # Convert to Google Sheets serial number (days since 1899-12-30)
                        epoch = datetime.datetime(1899, 12, 30)
                        delta = dt - epoch
                        filtered_row[key] = delta.days + (delta.seconds + delta.microseconds / 1e6) / 86400
                    except Exception:
                        pass
            filtered_data.append(filtered_row)

    # QA phone numbers using the global map
    if filtered_data:
        headers = list(filtered_data[0].keys())
        filtered_data = qa_phone_numbers_with_global_map(
            [list(row.values()) for row in filtered_data],
            headers,
            global_email_phone_map
        )
        data = [headers] + filtered_data
        
        try:
            worksheet.update(values=data, range_name='A1')
            # Add filter view and banding for table-like appearance
            num_rows = len(data)
            num_cols = len(data[0]) if data else 0
            if num_rows > 1 and num_cols > 0:
                requests = [
                    {
                        "addFilterView": {
                            "filter": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "startRowIndex": 0,
                                    "endRowIndex": num_rows,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": num_cols
                                }
                            }
                        }
                    }
                ]
                # Add date-time formatting for date columns
                for i, header in enumerate(headers):
                    if 'date' in header.lower():
                        requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "startRowIndex": 1,  # skip header
                                    "endRowIndex": num_rows,
                                    "startColumnIndex": i,
                                    "endColumnIndex": i + 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "numberFormat": {
                                            "type": "DATE_TIME"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.numberFormat"
                            }
                        })
                workbook.batch_update({"requests": requests})
            print(f"Imported {csv_file} to sheet {sheet_name} (columns filtered, phones normalized, table created)")
        except Exception as e:
            print(f"Error updating {sheet_name}: {e}")
            return
        # Skip formatting to reduce API calls
        # num_rows = len(data)
        # num_cols = len(data[0]) if data else 0
        # if num_rows > 0 and num_cols > 0:
        #     range_str = f'A1:{chr(ord("A") + num_cols - 1)}{num_rows}'
        #     requests = [{
        #         "range": range_str,
        #         "borders": {
        #             "top": {"style": "SOLID"},
        #             "bottom": {"style": "SOLID"},
        #             "left": {"style": "SOLID"},
        #             "right": {"style": "SOLID"},
        #             "innerHorizontal": {"style": "SOLID"},
        #             "innerVertical": {"style": "SOLID"}
        #         }
        #     }]
        #     workbook.batch_update({"requests": requests})

def build_global_email_phone_map(csv_dir, exclude_columns):
    email_phone_map = {}
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    for csv_file in csv_files:
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get('email', '').strip().lower()
                phone = row.get('phone', '').strip()
                digits = re.sub(r'\D', '', phone)
                if email and len(digits) == 10:
                    # Always prefer a valid phone if not already set
                    if email not in email_phone_map:
                        email_phone_map[email] = f"{digits[0:3]}.{digits[3:6]}.{digits[6:10]}"
    return email_phone_map

def sort_sheets_alphabetically(workbook):
    # Get all worksheets and their titles
    sheets = workbook.worksheets()
    # Sort by title
    sorted_sheets = sorted(sheets, key=lambda ws: ws.title.lower())
    # Reorder sheets in the workbook
    requests = []
    for idx, ws in enumerate(sorted_sheets):
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "index": idx
                },
                "fields": "index"
            }
        })
    if requests:
        workbook.batch_update({"requests": requests})

def main():
    # Load config
    cp = configparser.ConfigParser()
    cp.read('config.ini')
    if 'sheets' not in cp:
        print("Error: [sheets] section not found in config.ini")
        sys.exit(1)
    config = dict(cp['sheets'])
    
    credentials_file = config.get('credentials', 'credentials.json')
    scopes = config.get('scopes', 'https://www.googleapis.com/auth/drive').split(',')
    
    if not os.path.exists(credentials_file):
        print(f"Error: Credentials file {credentials_file} not found.")
        sys.exit(1)
    
    client = setup_google_sheets(credentials_file, scopes)
    workbook_name = "2025-2026-volunteers"
    workbook = get_or_create_workbook(client, workbook_name)
    
    csv_dir = "/home/ruddy/data/agimba/2025"
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        return
    
    # Build global email-to-phone map before processing files
    exclude_columns = {'amountpaid', 'slotitemid', 'hastime', 'status', 'starttime', 'startdate', 'phonetype', 'offset', 'endtime', 'itemmemberid', 'signupid', 'signedupdate', 'enddate', 'waitlist'}
    global_email_phone_map = build_global_email_phone_map(csv_dir, exclude_columns)
    
    for csv_file in csv_files:
        import_csv_to_sheet(workbook, csv_file, global_email_phone_map)
        time.sleep(1)  # Delay to avoid quota issues
    
    print(f"All CSVs imported to workbook '{workbook_name}'")
    # Sort sheets alphabetically
    sort_sheets_alphabetically(workbook)
    print("Sheets sorted alphabetically.")

if __name__ == "__main__":
    main()