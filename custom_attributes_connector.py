import csv
import sqlite3
import sys
import io
import re
from pathlib import Path
import argparse
import os
import requests
import json
import base64
from datetime import datetime
from typing import Dict, Any, Tuple, List, Iterator, Optional
from requests import Response
import time
from contextlib import redirect_stdout, redirect_stderr
from concurrent.futures import ThreadPoolExecutor, TimeoutError

#
# BEGIN Global Variables
#
global q_csv_file, q_database_file, q_api_fqdn, q_username, q_password, q_api_function
global csv_data_contract, axonious_table_name, payload_template_str, q_max_asset_ids, x_requested_with, q_api_endpoint
dry_run_flag = False
x_requested_with = 'custom_attributes_connector_v1.0'
q_api_endpoint = "/qps/rest/2.0/update/am/asset"
q_max_asset_ids = 100
q_csv_file = Path('./data/input.csv')
now = datetime.now()
timestamp = now.strftime('%Y%m%d_%H%M%S')
q_database_file = Path(f'custom_attributes_connector_sqlite_{timestamp}.db')
q_log_file = Path(f'custom_attributes_connector_log_{timestamp}.log')
axonious_table_name = 'axonious_data'
# Define a dictionary to map old headers to new headers
csv_data_contract = {'Qualys Scans: Qualys ID': 'AssetID',
                  'Mssql: MSSQL_ Business_Name': 'Business',
                  'Mssql: MSSQL_ Div_Name': 'Division',
                  'Mssql: MSSQL_ Productgroup': 'Product_Group',
                  'Mssql: MSSQL_ Portfolio': 'Portfolio',
                  'Mssql: MSSQL_ Product': 'Product',
                  'Mssql: MSSQL_ Primaryservice': 'Primary_Service',
                  'Mssql: MSSQL_ Serviceownergroup': 'Service_Owner_Group',
                  'Mssql: MSSQL_ Deviceownergroup': 'Device_Owner_Group',
                  'Mssql: MSSQL_ Recoverytier': 'Recovery_Tier',
                  'Mssql: MSSQL_ Sla': 'SLA'}

payload_template_str = json.dumps({
  "ServiceRequest": {
    "filters": {
      "Criteria": [
        {
          "field": "id",
          "operator": "IN",
          "value": "1545723886"
        }
      ]
    },
    "data": {
      "Asset": {
        "customAttributes": {
          "add": {
            "CustomAttribute": [
              {
                "key": "Business",
                "value": ""
              },
              {
                "key": "Division",
                "value": ""
              },
              {
                "key": "Product_Group",
                "value": ""
              },
              {
                "key": "Primary_Service",
                "value": ""
              },
              {
                "key": "Product",
                "value": ""
              },
              {
                "key": "Service_Onwer_Group",
                "value": ""
              },
              {
                "key": "Device_Owner_Group",
                "value": ""
              },
              {
                "key": "Recovery_Tier",
                "value": ""
              },
              {
                "key": "SLA",
                "value": ""
              }
            ]
          }
        }
      }
    }
  }
})

# exception classes
class WorkflowError(Exception):
    """Raised when a workflow step fails."""
    pass

class DatabaseInsertError(Exception):
    """Raised when failing to insert a payload into the database."""
    pass

#
# BEGIN Functions
#



def check_required_modules() -> None:
    """
    Check for all required third-party and standard library modules.
    Exits the program with clear instructions if any are missing.
    """

    # List of required third-party modules (pip-installable)
    required_pip_modules = [
        "requests",  # for HTTP API calls
    ]

    # Standard library modules (should always be available in CPython)
    stdlib_modules = [
        "csv",
        "sqlite3",
        "sys",
        "io",
        "re",
        "pathlib",
        "typing",
        "argparse",
        "os",
        "json",
        "base64",
        "datetime",
        "time",
    ]

    missing_modules = []

    # Check pip-installable modules
    for module_name in required_pip_modules:
        try:
            __import__(module_name)
        except ImportError:
            missing_modules.append((module_name, "pip"))

    # Check standard library modules (very rare to be missing, but safe)
    for module_name in stdlib_modules:
        try:
            __import__(module_name)
        except ImportError:
            missing_modules.append((module_name, "builtin"))

    # Report results
    if missing_modules:
        print("\n" + "=" * 60)
        print("ERROR: Missing required modules")
        print("=" * 60)
        print("The following modules are required but not installed:\n")

        for module, source in missing_modules:
            if source == "pip":
                print(f"  - {module:<15} → Run: pip install {module}")
            else:
                print(f"  - {module:<15} → Built-in module missing (check Python installation)")

        print("\nPlease install missing modules and try again.")
        print("=" * 60 + "\n")
        sys.exit(1)


def print_database_tables_help() -> None:
    """
    Prints detailed information about the database tables created by the application,
    including their purpose and schema.
    """
    print("""
Database Table Descriptions
==========================

The application creates several SQLite database tables to process and manage data from a CSV file,
generate Qualys API payloads, and log API execution results. Below is a description of each table,
its purpose, and its schema.

1. axonious_data
   - Purpose: Stores cleaned and transformed data from the input CSV file. Each row represents a
     single Qualys asset ID with associated custom attributes extracted from the CSV. The table is
     populated by parsing the CSV file, splitting multiple asset IDs into separate rows, and applying
     data cleansing (e.g., removing newlines, BOM characters, and trimming whitespace).
   - Schema:
     - qualys_id (TEXT): The Qualys asset ID for a specific asset.
     - Business (TEXT): The business name associated with the asset.
     - Division (TEXT): The division name associated with the asset.
     - Product_Group (TEXT): The product group associated with the asset.
     - Portfolio (TEXT): The portfolio associated with the asset.
     - Product (TEXT): The product name associated with the asset.
     - Primary_Service (TEXT): The primary service associated with the asset.
     - Service_Owner_Group (TEXT): The service owner group associated with the asset.
     - Device_Owner_Group (TEXT): The device owner group associated with the asset.
     - Recovery_Tier (TEXT): The recovery tier associated with the asset.
     - SLA (TEXT): The service level agreement associated with the asset.

2. qualys_attribute_payloads
   - Purpose: Stores initial Qualys API payloads for each asset ID, along with their custom attributes.
     This table is populated by iterating over the axonious_data table, generating JSON payloads for
     each asset ID, and storing the custom attributes as a JSON string. It serves as an intermediate
     step before grouping and splitting payloads.
   - Schema:
     - asset_id (TEXT): The Qualys asset ID.
     - payload (TEXT): The JSON string representing the Qualys API payload for updating the asset's
       custom attributes.
     - payload_custom_attributes (TEXT): The JSON string containing the list of custom attributes
       (key-value pairs) to be applied to the asset.

3. qualys_attribute_payloads_duplicates
   - Purpose: Identifies and stores rows from the qualys_attribute_payloads table where asset IDs
     appear multiple times, indicating potential duplicates. This table is used for data validation
     and debugging to ensure no asset ID is processed with conflicting custom attributes.
   - Schema:
     - asset_id (TEXT): The Qualys asset ID (cleaned of BOM characters).
     - payload (TEXT): The JSON string representing the Qualys API payload for the asset.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       asset.
       
3.1 qualys_attribute_payloads_clean
   - Purpose: deduplicated rows from the qualys_attribute_payloads table where asset IDs
     that appear multiple times are removed prior to processing. 
     This table is used for next step to create the qualys_attribute_payloads_grouped table.
   - Schema:
     - asset_id (TEXT): The Qualys asset ID (cleaned of BOM characters).
     - payload (TEXT): The JSON string representing the Qualys API payload for the asset.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       asset.

4. qualys_attribute_payloads_grouped
   - Purpose: Groups rows from the qualys_attribute_payloads table by unique sets of custom
     attributes, aggregating asset IDs into a comma-separated list. Each row represents a unique
     combination of custom attributes with associated asset IDs, along with a count of asset IDs
     and a group number for tracking.
   - Schema:
     - asset_ids (TEXT): A comma-separated list of Qualys asset IDs sharing the same custom attributes.
     - payload (TEXT): The JSON string representing the Qualys API payload for the group.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       group.
     - count_asset_ids (INTEGER): The number of asset IDs in the asset_ids field.
     - group_number (INTEGER): A unique identifier for each group, based on the row ID.

5. qualys_attribute_payloads_split
   - Purpose: Splits rows from the qualys_attribute_payloads_grouped table into multiple rows if the
     number of asset IDs exceeds a specified limit (default: 5). Each row contains at most the
     specified number of asset IDs, ensuring API calls are manageable. It includes a batch number to
     track splits within the same group.
   - Schema:
     - asset_ids (TEXT): A comma-separated list of Qualys asset IDs (up to the specified limit).
     - payload (TEXT): The JSON string representing the Qualys API payload for the batch.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       batch.
     - count_asset_ids (INTEGER): The number of asset IDs in the asset_ids field for this batch.
     - group_number (INTEGER): The group number inherited from the qualys_attribute_payloads_grouped
       table.
     - batch_number (INTEGER): A sequential number (starting from 1) for each batch within a group.

6. qualys_attribute_payloads_transformed
   - Purpose: Stores transformed payloads from the qualys_attribute_payloads_split table, where the
     payload JSON is updated to reflect the exact asset IDs and custom attributes for each batch.
     This table ensures payloads are correctly formatted for Qualys API calls.
   - Schema:
     - asset_ids (TEXT): A comma-separated list of Qualys asset IDs for the batch.
     - payload (TEXT): The transformed JSON string representing the Qualys API payload, with updated
       asset IDs and custom attributes.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       batch.
     - count_asset_ids (INTEGER): The number of asset IDs in the asset_ids field.
     - group_number (INTEGER): The group number inherited from the previous table.
     - batch_number (INTEGER): The batch number inherited from the previous table.

7. qualys_attribute_payloads_transformed_execution_log
   - Purpose: Logs the execution status of Qualys API calls for each transformed payload. It copies
     data from the qualys_attribute_payloads_transformed table and adds fields for tracking API call
     status and logs. Initially, status and execution_log are set to 'none', as API calls are not
     executed in the provided code.
   - Schema:
     - asset_ids (TEXT): A comma-separated list of Qualys asset IDs for the batch.
     - payload (TEXT): The transformed JSON string representing the Qualys API payload.
     - payload_custom_attributes (TEXT): The JSON string containing the custom attributes for the
       batch.
     - count_asset_ids (INTEGER): The number of asset IDs in the asset_ids field.
     - group_number (INTEGER): The group number inherited from the previous table.
     - batch_number (INTEGER): The batch number inherited from the previous table.
     - status (TEXT): The status of the API call (initially 'none').
     - execution_log (TEXT): The log of the API call execution (initially 'none').
""")

def print_usage() -> None:
    """
    Prints usage instructions for the script, including command-line arguments, environment variables,
    and the CSV data contract.
    """
    # Pretty print csv_data_contract
    max_field_length = max(len(field) for field in csv_data_contract.keys())
    max_mapped_length = max(len(mapped) for mapped in csv_data_contract.values())
    contract_table = "\n".join(
        f"  {field:<{max_field_length}} -> {mapped:<{max_mapped_length}}"
        for field, mapped in csv_data_contract.items()
    )

    print(f"""
Usage: python custom_attributes_connector.py [options]

This program processes an Axonious CSV file, to produce API calls that will add custom attributes
to each Qualys asset id referenced in Axonius CSV File.  The CSV Contract and Cleansing transforms
are detailed below. The API Endpoint used for this application is {q_api_endpoint}.  The application 
will throttle itself and will wait and retry in the event of concurrency or connectivity errors.

Options:
  -c, --csv-file PATH      Path to the input CSV file (required, must exist, default: {q_csv_file})
  -d, --dry-run            Do not execute any API calls.
  --db-help                Show database table descriptions and exit
  -a, --api-fqdn FQDN      Qualys API fully qualified domain name (default: qualysapi.qg3.apps.qualys.com)
  -f, --api-function FUNC  Qualys API function to perform on assets custom attributes: add, update, or remove (default: add)
                           - add    - add custom attribute key/data pair from csv-file if the key does not exist.
                           - update - update custom attribute key/data pair if the key exists.
                           - remove - remove custom attribute key/data pair if the key exists.
  -h, --help               Show this help message and exit

Environment Variables:
  q_csv_file               Alternative to --csv-file (must point to an existing file)
  q_api_fqdn               Alternative to --api-fqdn (default: qualysapi.qg3.apps.qualys.com)
  q_api_function           Alternative to --api-function (default: add, must be add, update, or remove)
  q_username               Qualys API user ID (required)
  q_password               Qualys API password (required)

CSV Data Contract:
  Each value in the input data is cleaned as part of transform.
   1) Newlines are replaced with commas
   2) Trailing or leading whitespace characters are removed
   3) Byte Order Mark (BOM) characters are removed (character '\\ufeff')
   4) Records with multiple asset IDs result in a record for each asset ID to assign key/data custom attributes

  The input CSV file must contain the following fields, which are mapped to the specified keys:
{contract_table}

Examples:
  #
  # Note you may have python or python3 depending on your installation.
  # 
  Linux (Bash):
    export q_username=your_username 
    export q_password=your_password 
    python3 custom_attributes_connector.py --csv-file input.csv --api-fqdn qualysapi.qg3.apps.qualys.com -api-function add

  Command-line arguments:
    python3 custom_attributes_connector.py --csv-file input.csv --api-fqdn qualysapi.qg3.apps.qualys.com --api-function add

Note:
  You must set q_username to your Qualys API user ID and q_password to your Qualys API password as environment variables.
    """)



def get_config() -> tuple[Path, Path, str, str, str, str, bool]:
    """
    Processes command-line arguments and environment variables to configure file paths and Qualys API settings.

    Returns:
        tuple[Path, Path, str, str, str, str]: Paths to the input CSV file, SQLite database file, Qualys API FQDN,
                                              API function, username, and password.

    Raises:
        ValueError: If any required configurations are invalid or missing.
    """
    # Default values
    default_csv_file = q_csv_file
    _db_file = q_database_file
    _dry_run = False
    default_api_fqdn = 'qualysapi.qg3.apps.qualys.com'
    default_api_function = 'add'

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process Qualys CSV data and update SQLite database with attribute payloads.",
        add_help=False  # Disable default help to use custom print_usage
    )
    parser.add_argument(
        '-c', '--csv-file',
        type=Path,
        default=default_csv_file,
        help='Path to the input CSV file (must exist)'
    )
    parser.add_argument(
        '-a', '--api-fqdn',
        type=str,
        default=default_api_fqdn,
        help='Qualys API fully qualified domain name (default: qualysapi.qg3.apps.qualys.com)'
    )
    parser.add_argument(
        '-f', '--api-function',
        type=str,
        default=default_api_function,
        help='Qualys API function to perform on custom attributes: add, update, or remove (default: add)'
    )
    parser.add_argument(
        '-h', '--help',
        action='store_true',
        help='Show general help message and exit'
    )
    parser.add_argument(
        '--db-help',
        action='store_true',
        help='Show database table descriptions and exit'
    )
    parser.add_argument(
        '-d','--dry-run',
        action='store_true',
        help='Dry Run, create database but do not run API calls.'
    )

    # Parse arguments
    args = parser.parse_args()

    # Show usage and exit if help is requested
    if args.help:
        print_usage()
        sys.exit(0)

    if 'dry_run' in args:
        if args.dry_run is True:
            _dry_run = True
        else:
            _dry_run = False


    # Show database table help and exit if requested
    if args.db_help:
        print_database_tables_help()
        sys.exit(0)

    # Check environment variables if arguments are not provided or are defaults
    _csv_file = args.csv_file
    if str(_csv_file) == str(default_csv_file):
        env_csv = os.getenv('q_csv_file')
        if env_csv:
            _csv_file = Path(env_csv)

    _api_fqdn = args.api_fqdn
    if _api_fqdn == default_api_fqdn:
        env_api_fqdn = os.getenv('q_api_fqdn')
        if env_api_fqdn:
            _api_fqdn = env_api_fqdn

    _api_function = args.api_function
    if _api_function == default_api_function:
        env_api_function = os.getenv('q_api_function')
        if env_api_function:
            _api_function = env_api_function

    # Collect all errors
    errors = []

    # Validate Qualys API credentials
    _q_username = os.getenv('q_username')
    _q_password = os.getenv('q_password')
    if not _q_username:
        errors.append("Missing required environment variable q_username, which must be set to your Qualys API user ID")
    if not _q_password:
        errors.append("Missing required environment variable q_password, which must be set to your Qualys API password")

    # Validate API function
    valid_functions = {'add', 'update', 'remove'}
    if _api_function.lower() not in valid_functions:
        errors.append(f"Invalid Qualys API function '{_api_function}'; must be one of: {', '.join(valid_functions)}. Provide via --api-function or q_api_function.")

    # Validate CSV file (mandatory, must exist)
    if not _csv_file.exists():
        errors.append(f"Missing or invalid CSV file path: {_csv_file} does not exist. Provide via --csv-file or q_csv_file.")

    # Validate database file path (may not exist, but parent directory must be writable)
    if not os.access(_db_file.parent, os.W_OK):
        errors.append(f"Invalid database file path: parent directory {_db_file.parent} is not writable. ")

    # If there are errors, print usage and raise ValueError with all messages
    if errors:
        print_usage()
        error_message = "Configuration errors:\n" + "\n".join(f"  * {error}" for error in errors)
        print(error_message)
        sys.exit(1)

    return _csv_file, _db_file, _api_fqdn, _q_username, _q_password, _api_function.lower(), _dry_run


def iterate_over_csv_rows_returning_one_row_at_a_time(file_path: str | Path) -> Iterator[Dict[str, Any]]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            # Read the first line manually to extract and clean fieldnames
            first_line = file.readline()
            if not first_line:
                raise ValueError("CSV file is empty")

            # Parse the first line as CSV to get raw fieldnames
            header_reader = csv.reader(io.StringIO(first_line))
            fieldnames = next(header_reader)

            if not fieldnames:
                raise ValueError("CSV file has no headers")

            cleaned_fieldnames = []
            for name in fieldnames:
                # Remove BOM and control characters
                cleaned_name = ''.join(char for char in name if char != '\ufeff')
                cleaned_name = cleaned_name.strip()
                cleaned_fieldnames.append(cleaned_name)

            # Validate that all required fields from csv_data_contract exist
            required_fields = set(csv_data_contract.keys())
            actual_fields = set(cleaned_fieldnames)
            missing_fields = required_fields - actual_fields
            if missing_fields:
                raise ValueError(f"Broken Contract, input CSV file missing required fields: {', '.join(sorted(missing_fields))}")

            # Now create DictReader with cleaned fieldnames, reading from the remaining file content
            reader = csv.DictReader(file, fieldnames=cleaned_fieldnames)

            for _row in reader:
                yield dict(_row)
    except csv.Error as e:
        raise csv.Error(f"Error parsing CSV file: {e}")


def create_axonius_table(_csv_data_file):
    try:
        # Open the SQLite database
        with sqlite3.connect(q_database_file) as conn:
            cursor = conn.cursor()

            cursor.execute(f'''DROP TABLE IF EXISTS {axonious_table_name}''')
            # Create the table if it doesn't exist (using simplified column names)
            cursor.execute(f'''CREATE TABLE IF NOT EXISTS {axonious_table_name} 
                              (
                                  qualys_id
                                  TEXT,
                                  Business
                                  TEXT,
                                  Division
                                  TEXT,
                                  Product_Group
                                  TEXT,
                                  Portfolio
                                  TEXT,
                                  Product
                                  TEXT,
                                  Primary_Service
                                  TEXT,
                                  Service_Owner_Group
                                  TEXT,
                                  Device_Owner_Group
                                  TEXT,
                                  Recovery_Tier
                                  TEXT,
                                  SLA
                                  TEXT
                              )''')

            # Iterate over CSV rows one at a time and insert into the database
            for _row in iterate_over_csv_rows_returning_one_row_at_a_time(_csv_data_file):
                qualys_asset_ids = re.sub(r'\n+', ',', _row.get('Qualys Scans: Qualys ID', '').strip())
                asset_id_list = [asset_id.strip() for asset_id in qualys_asset_ids.split(',') if asset_id.strip()]

                for asset_id in asset_id_list:
                    cursor.execute(
                        f'''INSERT INTO {axonious_table_name} (qualys_id,
                                             Business, Division, Product_Group, Portfolio,
                                             Product, Primary_Service, Service_Owner_Group, Device_Owner_Group,
                                             Recovery_Tier, SLA)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (
                            asset_id,
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Business_Name', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Div_Name', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Productgroup', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Portfolio', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Product', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Primaryservice', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Serviceownergroup', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Deviceownergroup', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Recoverytier', '').strip()) or "",
                            re.sub(r'\n+', ',', _row.get('Mssql: MSSQL_ Sla', '').strip()) or ""
                        ))

            # Commit all inserts after processing all rows
            conn.commit()
            print("Data successfully inserted into the database.")

    except (FileNotFoundError, ValueError, csv.Error, sqlite3.Error, DatabaseInsertError) as e:
        print(f"Error: {e}")
        raise WorkflowError(f"Error: {e}") from e


def create_payloads_table(_db_path: Path) -> None:
    conn = sqlite3.connect(_db_path)
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS qualys_attribute_payloads''')
    cursor.execute('''CREATE TABLE qualys_attribute_payloads (asset_id TEXT, payload TEXT, payload_custom_attributes TEXT)''')
    conn.commit()
    conn.close()


def iterate_over_axonious_rows(conn) -> Iterator[Dict[str, Any]]:
    try:
        # Connect to the database
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='axonious_data'")
        if not cursor.fetchone():
            raise sqlite3.Error("Table 'axonious_data' does not exist in the database")

        # Query all rows and yield one at a time
        cursor.execute("SELECT * FROM axonious_data")
        for _row in cursor:
            yield dict(_row)

    except sqlite3.Error as e:
        raise sqlite3.Error(f"Database error: {e}")


def print_axonious_data(_db_path: Path) -> None:
    for row_idx, row in enumerate(iterate_over_axonious_rows(_db_path), 1):
        print(f"\nRow {row_idx}:")
        for key, value in row.items():
            print(f"  {key}: {value}")


def insert_qualys_payload(cursor, asset_id, payload, payload_custom_attributes):
    try:
        cursor.execute('''
            INSERT INTO qualys_attribute_payloads (asset_id, payload, payload_custom_attributes)
            VALUES (?, ?, ?)
        ''', (asset_id, payload, payload_custom_attributes))
    except Exception as e:
        print(f"Error inserting row: {e}")
        error_msg = f"Failed to insert asset_id={asset_id}: {e}"
        raise DatabaseInsertError(error_msg) from e



def create_payload(asset_ids: str, _custom_attributes: list) -> tuple[str, str]:
    payload_template_dict = json.loads(payload_template_str)
    criteria = payload_template_dict['ServiceRequest']['filters']['Criteria']
    criteria[0]['value'] = re.sub(r'\n+', ',', asset_ids.strip())

    payload_custom_attributes \
        = payload_template_dict['ServiceRequest']['data']['Asset']['customAttributes']['add']['CustomAttribute']
    payload_custom_attributes = []
    for i, attr in enumerate(_custom_attributes):
        if attr['key'] != 'qualys_id':
            if attr['value'] != '':
                payload_custom_attributes.append({'key': attr['key'], 'value': attr['value']})
            # payload_template_dict['ServiceRequest']['data']['Asset']['customAttributes']['add']['CustomAttribute'][i]['key'] = _custom_attributes[i]['key']
            # payload_template_dict['ServiceRequest']['data']['Asset']['customAttributes']['add']['CustomAttribute'][i]['value'] = _custom_attributes[i]['value']
    payload_str = json.dumps(payload_template_dict)
    payload_custom_attributes_str = json.dumps(payload_custom_attributes)
    return payload_str, payload_custom_attributes_str


def populate_payloads_table(_db_path: Path):
    count = 0
    with sqlite3.connect(_db_path) as conn:
        cursor = conn.cursor()
        for _row_idx, _row in enumerate(iterate_over_axonious_rows(conn), 1):
            row_data = []
            for key, value in _row.items():
                row_data.append({"key": key, "value": value})
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} rows")
                conn.commit()
            #print(f"\nRow {row_data}:")
            qualys_asset_ids = _row.get('qualys_id', '').strip()
            payload, payload_custom_attributes = create_payload(qualys_asset_ids, row_data)
            insert_qualys_payload(cursor, qualys_asset_ids, payload, payload_custom_attributes)
        if count % 1000 != 0:
            print(f"Processed {count} rows, committing remaining changes")
            conn.commit()


def create_group_payloads_by_asset_table(_db_path: Path,
                                         new_table_name: str = "qualys_attribute_payloads_grouped") -> None:
    """Group Qualys payloads into asset ID lists by unique key-data pairs and add group numbers.

    Args:
        _db_path (Path): Path to the SQLite database file.
        new_table_name (str): Name of the table to create (default: 'qualys_attribute_payloads_grouped').

    Raises:
        FileNotFoundError: If the database file does not exist.
        sqlite3.Error: If a database error occurs.
        Exception: For other unexpected errors.
    """
    if not _db_path.exists():
        raise WorkflowError(f"Database file not found: {_db_path}")

    try:
        print(f"Creating '{new_table_name}' from qualys_attribute_payloads table...")
        with sqlite3.connect(_db_path) as conn:
            cursor = conn.cursor()

            cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")
            # Create or replace the new table with group_number as INTEGER
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {new_table_name} (
                    asset_ids TEXT,
                    payload TEXT,
                    payload_custom_attributes TEXT,
                    count_asset_ids INTEGER,
                    group_number INTEGER
                )
            """)

            # Clear the table if it already exists
            cursor.execute(f"DELETE FROM {new_table_name}")

            # Insert grouped data (excluding group_number for now)
            cursor.execute("""
                INSERT INTO {new_table_name} (asset_ids, payload, payload_custom_attributes, count_asset_ids)
                SELECT 
                    GROUP_CONCAT(REPLACE(COALESCE(asset_id, ''), char(65279), ''), ',') AS asset_ids,
                    MAX(payload) AS payload,
                    REPLACE(payload_custom_attributes, char(65279), '') AS payload_custom_attributes,
                    COUNT(asset_id) AS count_asset_ids
                FROM qualys_attribute_payloads_clean
                GROUP BY REPLACE(payload_custom_attributes, char(65279), '')
            """.format(new_table_name=new_table_name))

            # Add group numbers as integers starting from 1
            cursor.execute(f"""
                UPDATE {new_table_name}
                SET group_number = (
                    SELECT rowid
                    FROM {new_table_name} AS t
                    WHERE t.rowid = {new_table_name}.rowid
                )
            """)

            # Commit the changes
            conn.commit()
            print(f"Created or replaced table '{new_table_name}' with {cursor.rowcount} rows.")

    except sqlite3.Error as e:
        error_msg = f"SQLite error in '{new_table_name}' creation: {e}"
        raise WorkflowError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error creating grouped table '{new_table_name}': {e}"
        raise WorkflowError(error_msg) from e


def create_non_duplicate_payload_table(
        _db_path: Path,
        main_table="qualys_attribute_payloads",
        dup_table="qualys_attribute_payloads_duplicates",
        new_table="qualys_attribute_payloads_clean",
        case_insensitive=True
):
    """
    Creates a new table with non-duplicate rows from main_table (excluding asset_ids in dup_table).
    Drops new_table if it already exists.

    Args:
        _db_path (Path): Path to SQLite database
        main_table (str): Source table
        dup_table (str): Table with duplicates to exclude
        new_table (str): Output clean table
        case_insensitive (bool): Match asset_id case-insensitively (recommended for TEXT)

    Returns:
        int: Number of rows inserted
    """
    conn = sqlite3.connect(_db_path)
    cursor = conn.cursor()

    try:
        # Step 1: Drop new table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {new_table}")
        print(f"Dropped table if existed: {new_table}")

        # Step 2: Create new table with same schema (empty)
        cursor.execute(f"""
            CREATE TABLE {new_table} (
                asset_id                  TEXT,
                payload                   TEXT,
                payload_custom_attributes TEXT
            )
        """)
        print(f"Created table: {new_table}")

        # Step 3: Build JOIN condition
        if case_insensitive:
            join_condition = "UPPER(TRIM(m.asset_id)) = UPPER(TRIM(d.asset_id))"
        else:
            join_condition = "m.asset_id = d.asset_id"

        # Step 4: Insert non-duplicate rows
        insert_query = f"""
            INSERT INTO {new_table} (asset_id, payload, payload_custom_attributes)
            SELECT m.asset_id, m.payload, m.payload_custom_attributes
            FROM {main_table} m
            LEFT JOIN {dup_table} d ON {join_condition}
            WHERE d.asset_id IS NULL
        """
        cursor.execute(insert_query)
        rows_inserted = cursor.rowcount

        conn.commit()
        print(f"Success: Inserted {rows_inserted} non-duplicate rows into {new_table}")
        return rows_inserted

    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise
    finally:
        conn.close()


def create_split_payloads_table(_db_path: Path, max_asset_ids: int = 100,
                               new_table_name: str = "qualys_attribute_payloads_split") -> None:
    """
    Creates or replaces a SQLite table from qualys_attribute_payloads_grouped, splitting
    rows with more than max_asset_ids (default 100) into multiple rows, each with at most
    max_asset_ids asset_ids. Updates count_asset_ids to reflect the number of asset_ids
    in each row. Includes group_number from the source table and assigns batch_number
    starting from 1 for each group.

    Args:
        _db_path (Path): Path to the SQLite database file.
        max_asset_ids (int): Maximum number of asset_ids per row (default: 100).
        new_table_name (str): Name of the new table (default: qualys_attribute_payloads_split).

    Raises:
        FileNotFoundError: If the database file does not exist.
        sqlite3.Error: If a database error occurs.
        Exception: For other unexpected errors.
    """
    _db_path = Path(_db_path)
    if not _db_path.exists():
        raise FileNotFoundError(f"Database file not found: {_db_path}")

    def split_into_chunks(_asset_ids: str, max_size: int) -> List[str]:
        """Splits a comma-separated string into chunks of at most max_size items."""
        if not _asset_ids or _asset_ids.strip() == '':
            return ['']
        # Clean BOM and extra whitespace
        _asset_ids = _asset_ids.replace('\ufeff', '').strip()
        ids_list = [id.strip() for id in _asset_ids.split(',') if id.strip()]
        if not ids_list:  # Handle case where split results in empty list
            return ['']
        return [','.join(ids_list[i:i + max_size]) for i in range(0, len(ids_list), max_size)]

    try:
        print(f"Creating '{new_table_name}' from qualys_attribute_payloads_grouped table...")
        with sqlite3.connect(_db_path) as conn:
            conn.execute("PRAGMA foreign_keys=OFF")  # Disable foreign keys for performance
            cursor = conn.cursor()

            # Drop and recreate the table to ensure correct schema
            cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")
            cursor.execute(f"""
                CREATE TABLE {new_table_name} (
                    asset_ids TEXT,
                    payload TEXT,
                    payload_custom_attributes TEXT,
                    count_asset_ids INTEGER,
                    group_number INTEGER,
                    batch_number INTEGER
                )
            """)

            # Fetch rows from qualys_attribute_payloads_grouped
            cursor.execute("""
                SELECT asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number
                FROM qualys_attribute_payloads_grouped
            """)
            all_rows = cursor.fetchall()  # Fetch all rows to avoid cursor conflict

            rows_fetched = len(all_rows)
            rows_inserted = 0
            for row_num, _row in enumerate(all_rows, 1):
                asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number = _row
                # Handle None values
                asset_ids = asset_ids or ''
                print(f"Processing row {row_num}/{rows_fetched}: group_number={group_number}, "
                      f"asset_ids length={len(asset_ids)}, count_asset_ids={count_asset_ids}, "
                      f"attributes='{payload_custom_attributes}'")

                # Split asset_ids into chunks
                chunks = split_into_chunks(asset_ids, max_asset_ids)
                print(f"  Split into {len(chunks)} chunks")
                for batch_num, chunk in enumerate(chunks, 1):  # Start batch_number at 1 for each group
                    chunk_count = len(chunk.split(',')) if chunk and chunk.strip() else 0
                    cursor.execute(
                        f"INSERT INTO {new_table_name} (asset_ids, payload, payload_custom_attributes, "
                        "count_asset_ids, group_number, batch_number) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (chunk, payload, payload_custom_attributes, chunk_count, group_number, batch_num)
                    )
                    rows_inserted += 1
                    print(f"  Inserted chunk with {chunk_count} asset_ids, group_number={group_number}, "
                          f"batch_number={batch_num}, total inserted: {rows_inserted}")

                # Commit every 1000 inserted rows
                if rows_inserted % 1000 == 0:
                    conn.commit()
                    print(f"Committed {rows_inserted} rows to {new_table_name}")

            # Final commit
            if rows_inserted % 1000 != 0:
                conn.commit()
                print(f"Final commit: Inserted {rows_inserted} rows to {new_table_name}")

            print(f"Completed: Fetched {rows_fetched} rows, inserted {rows_inserted} rows")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.execute("PRAGMA foreign_keys=ON")  # Re-enable foreign keys


def create_transform_payloads_table(_db_path: Path, new_table_name: str = "qualys_attribute_payloads_transformed") -> None:
    _db_path = Path(_db_path)
    if not _db_path.exists():
        raise FileNotFoundError(f"Database file not found: {_db_path}")

    def transform_payload(payload_json: str, _asset_ids: str, custom_attributes_json: str) -> str:
        """Transforms the payload JSON by updating value and CustomAttribute fields."""
        try:
            # Parse JSON
            _payload = json.loads(payload_json) if payload_json else {}
            custom_attributes = json.loads(custom_attributes_json) if custom_attributes_json else []

            # Validate payload structure
            if not isinstance(_payload, dict) or "ServiceRequest" not in _payload:
                raise ValueError("Invalid _payload JSON structure")

            # Update _asset_ids in filters.Criteria
            cleaned_ids = [_id.strip().replace('\ufeff', '') for _id in asset_ids.split(',') if
                           _id.strip()] if asset_ids else []
            asset_ids_str = ','.join(cleaned_ids)
            criteria = _payload.get("ServiceRequest", {}).get("filters", {}).get("Criteria", [])
            if criteria and isinstance(criteria, list) and len(criteria) > 0:
                criteria[0]["value"] = asset_ids_str

            # Update CustomAttribute
            custom_attr_path = _payload.get("ServiceRequest", {}).get("data", {}).get("Asset", {}).get("customAttributes", {}).get("add", {})
            if custom_attr_path:
                custom_attr_path["CustomAttribute"] = custom_attributes

            return json.dumps(_payload)
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            raise
        except Exception as e:
            print(f"Error transforming payload: {e}")
            raise

    try:
        print(f"Creating '{new_table_name}' from qualys_attribute_payloads_split table...")
        with sqlite3.connect(_db_path) as conn:
            conn.execute("PRAGMA foreign_keys=OFF")  # Disable foreign keys for performance
            cursor = conn.cursor()

            cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")

            # Create or replace the new table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {new_table_name} (
                    asset_ids TEXT,
                    payload TEXT,
                    payload_custom_attributes TEXT,
                    count_asset_ids INTEGER,
                    group_number INTEGER,
                    batch_number INTEGER
                )
            """)
            cursor.execute(f"DELETE FROM {new_table_name}")  # Clear existing data

            # Fetch all rows to avoid cursor conflict
            cursor.execute("""
                SELECT asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number
                FROM qualys_attribute_payloads_split
            """)
            all_rows = cursor.fetchall()

            rows_fetched = len(all_rows)
            rows_inserted = 0
            for row_num, _row in enumerate(all_rows, 1):
                asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number = _row
                asset_ids = asset_ids or ''  # Handle None
                print(f"Processing row {row_num}/{rows_fetched}: asset_ids length={len(asset_ids)}, "
                      f"count_asset_ids={count_asset_ids}")

                # Transform the payload
                try:
                    transformed_payload = transform_payload(payload, asset_ids, payload_custom_attributes)
                except Exception as e:
                    print(f"Skipping row {row_num} due to transformation error: {e}")
                    continue

                # Insert transformed row
                cursor.execute(
                    f"INSERT INTO {new_table_name} (asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (asset_ids, transformed_payload, payload_custom_attributes, count_asset_ids, group_number, batch_number)
                )
                rows_inserted += 1
                print(f"  Inserted row, total inserted: {rows_inserted}")

                # Commit every 1000 rows
                if rows_inserted % 1000 == 0:
                    conn.commit()
                    print(f"Committed {rows_inserted} rows to {new_table_name}")

            # Final commit
            if rows_inserted % 1000 != 0:
                conn.commit()
                print(f"Final commit: Inserted {rows_inserted} rows to {new_table_name}")

            print(f"Completed: Fetched {rows_fetched} rows, inserted {rows_inserted} rows")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.execute("PRAGMA foreign_keys=ON")  # Re-enable foreign keys


def create_payloads_duplicates_table(_db_path: Path, source_table: str = "qualys_attribute_payloads",
                                     new_table_name: str = "qualys_attribute_payloads_duplicates") -> None:
    if not _db_path.exists():
        raise FileNotFoundError(f"Database file not found: {_db_path}")

    try:
        with sqlite3.connect(_db_path) as conn:
            cursor = conn.cursor()

            # Drop the table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")

            # Create the new table with duplicates, handling BOM in asset_id
            cursor.execute(f"""
                CREATE TABLE {new_table_name} AS
                SELECT *
                FROM {source_table}
                WHERE REPLACE(COALESCE(asset_id, ''), char(65279), '') IN (
                    SELECT REPLACE(COALESCE(asset_id, ''), char(65279), '') AS cleaned_asset_id
                    FROM {source_table}
                    GROUP BY cleaned_asset_id
                    HAVING COUNT(*) > 1
                )
            """)

            # Commit changes
            conn.commit()

            # Count rows in the new table for feedback
            cursor.execute(f"SELECT COUNT(*) FROM {new_table_name}")
            row_count = cursor.fetchone()[0]
            print(f"Created '{new_table_name}' with {row_count} duplicate rows.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


def update_custom_attribute_operation(_json_data_str, operation):
    """
    Updates the 'add' operation in customAttributes to 'update' or 'remove'.

    Args:
        _json_data_str (str): The original ServiceRequest JSON as a Python dict.
        operation (str): Either 'update' or 'remove' or 'add'

    Returns:
        dict: Updated JSON structure with modified operation.

    Raises:
        ValueError: If operation is not 'update' or 'remove'
    """
    if operation not in ["update", "remove", "add"]:
        raise ValueError("Operation must be 'update' or 'remove' or 'add'")

    # Deep copy to avoid modifying original
    updated_data = json.loads(_json_data_str)

    # Navigate to the add section
    try:
        custom_attrs = updated_data["ServiceRequest"]["data"]["Asset"]["customAttributes"]

        if "add" in custom_attrs:
            # Move the content from 'add' to the new operation
            custom_attrs[operation] = custom_attrs.pop("add")

        json_data_str = json.dumps(updated_data)
        return json_data_str

    except KeyError as e:
        raise KeyError(f"Expected key not found in JSON structure: {e}")


def execute_api_calls_into_execution_log(
    _db_path: Path,
    source_table: str = "qualys_attribute_payloads_transformed",
    new_table_name: str = "qualys_attribute_payloads_transformed_execution_log",
    _q_username: str = "",
    _q_password: str = "",
    _q_api_fqdn: str = "",
    _q_api_endpoint: str = "",
    _q_api_function: str = "",
    _dry_run: bool = False
) -> None:
    """
    Creates or replaces a new SQLite table 'qualys_attribute_payloads_transformed_execution_log' and inserts
    all rows from qualys_attribute_payloads_transformed, leaving status and execution_log blank.

    Args:
        _db_path (Path): Path to the SQLite database file.
        source_table (str): Name of the source table (default: qualys_attribute_payloads_transformed).
        new_table_name (str): Name of the new table (default: qualys_attribute_payloads_transformed_execution_log).

    Raises:
        FileNotFoundError: If the database file does not exist.
        sqlite3.Error: If a database error occurs (e.g., table not found).
        :param new_table_name:
        :param source_table:
        :param _db_path:
        :param _q_api_fqdn:
        :param _q_password:
        :param _q_username:
        :param _q_api_function:
        :param _q_api_endpoint:
        :param _dry_run:

    """
    _db_path = Path(_db_path)
    if not _db_path.exists():
        raise FileNotFoundError(f"Database file not found: {_db_path}")

    try:
        with (sqlite3.connect(_db_path) as conn):
            conn.execute("PRAGMA foreign_keys=OFF")  # Disable foreign keys for performance
            cursor = conn.cursor()

            # Drop and create the new table
            cursor.execute(f'DROP TABLE IF EXISTS {new_table_name}')
            cursor.execute(f"""
                CREATE TABLE {new_table_name} (
                    asset_ids TEXT,
                    payload TEXT,
                    payload_custom_attributes TEXT,
                    count_asset_ids INTEGER,
                    group_number INTEGER,
                    batch_number INTEGER,
                    status TEXT,
                    execution_log TEXT
                )
            """)

            # Select rows from source table and fetch all to avoid cursor conflict
            cursor.execute(f"""
                SELECT asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number
                FROM {source_table}
            """)
            all_rows = cursor.fetchall()

            print(f"Inserting rows from {source_table} into {new_table_name}...")
            print(f"Payload function is {_q_api_function} for API {_q_api_fqdn}{_q_api_endpoint}. ...")
            rows_inserted = 0
            for row in all_rows:
                rows_inserted += 1
                asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number = row
                # Handle None values and clean BOM
                asset_ids = asset_ids.replace('\ufeff', '') if asset_ids else ''
                payload = payload if payload is not None else ''
                payload_custom_attributes = payload_custom_attributes.replace('\ufeff', '') if payload_custom_attributes else ''
                payload = update_custom_attribute_operation(_json_data_str=payload, operation=_q_api_function)
                count_asset_ids = count_asset_ids if count_asset_ids is not None else 0

                if _dry_run:
                    print(f"Dry run flag is set to {_dry_run}.  Create databases, and do not run API calls")
                    response = None
                else:
                    response = update_qualys_assets(
                    _q_username=_q_username,
                    _q_password=_q_password,
                    _q_api_fqdn=_q_api_fqdn,
                    _q_api_endpoint=_q_api_endpoint,
                    _payload=payload,
                    _group_number=group_number,
                    _batch_number=batch_number,
                    _q_api_function=_q_api_function)

                # Insert row with blank status and execution_log
                status = 'none'
                execution_log = 'none'
                if response:
                    execution_log = response.text
                    status = response.status_code
                    execution_log_message = re.sub(r' +', ' ', re.sub(r'[\r\n]+', '', execution_log)).strip()
                    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | "
                          f"API Call Number: {rows_inserted:>10,}, "
                          f"API Call Operation: {_q_api_function}, "
                          f"API Status: {status}. "
                          f"Execution log: {execution_log_message}")

                cursor.execute(
                    f"INSERT INTO {new_table_name} (asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number, status, execution_log) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (asset_ids, payload, payload_custom_attributes, count_asset_ids, group_number, batch_number, status, execution_log)
                )

                # Commit every 1000 rows
                if rows_inserted % 1000 == 0:
                    conn.commit()
                    print(f"Inserted {rows_inserted} rows into {new_table_name}")

            # Final commit
            if rows_inserted % 1000 != 0:
                conn.commit()
                print(f"Final commit: Inserted {rows_inserted} rows into {new_table_name}")

            if rows_inserted == 0:
                print(f"No rows found in {source_table}.")
            else:
                print(f"Completed: Inserted {rows_inserted} rows from {source_table} into {new_table_name}.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.execute("PRAGMA foreign_keys=ON")  # Re-enable foreign keys


def update_qualys_assets(
        _q_api_fqdn: str, _q_api_endpoint: str, _q_username: str, _q_password: str,
        _payload: Dict[str, Any], _q_api_function: str, _group_number: int, _batch_number: int) -> Response | None:
    """
    Sends a POST request to the Qualys API to update asset custom attributes using Basic authentication.

    Args:
        _q_api_fqdn (str): Fully qualified domain name of the Qualys API (e.g., qualysapi.qg3.apps.qualys.com).
        _q_api_endpoint (str):  API endpoint
        _q_username (str): Qualys API username.
        _q_password (str): Qualys API password.
        _payload (Dict[str, Any]): JSON-serializable dictionary containing the ServiceRequest _payload.
        _q_api_function (str): Function add, update, remove
        _group_number (int): group number for key/data pairs
        _batch_number (int): batch number for key/data pairs


    Returns:
        Response | None: The HTTP response object from the Qualys API, or None if the request fails
                        (e.g., due to a network error). The caller should check response.status_code
                        to handle errors.

    Raises:
        ValueError: If _q_api_fqdn, _q_username, or _q_password is empty.
    """
    # Validate inputs
    if not _q_api_fqdn:
        raise ValueError("Qualys API FQDN must be provided")
    if not _q_username:
        raise ValueError("Qualys API username must be provided")
    if not _q_password:
        raise ValueError("Qualys API password must be provided")

    # Construct URL using _q_api_fqdn
    url = f"https://{_q_api_fqdn}{_q_api_endpoint}"

    # Create Basic authentication header
    auth_header = get_basic_auth(_q_username, _q_password)

    user_agent_message = f"{x_requested_with} function={_q_api_function} group={_group_number} batch={_batch_number}"

    # Set headers
    headers = {
        'User-Agent': user_agent_message,
        'X-Requested-With': x_requested_with,
        'Content-Type': 'application/json',
        'Authorization': auth_header
    }

    # Set Retries for API Call.
    max_retries = 10
    max_delay = 300  # Maximum delay in seconds
    min_delay = 30  # Minimum delay in seconds

    # Calculate retry delays dynamically
    retry_delays = []
    if max_retries <= 1:
        retry_delays.append(min_delay)  # Single retry uses min_delay
    else:
        step = (max_delay - min_delay) / (max_retries - 1)  # Linear step size
        for i in range(max_retries):
            delay = min_delay + (step * i)  # Calculate delay for this attempt
            delay = min(int(delay), max_delay)  # Cap at max_delay and convert to integer
            retry_delays.append(delay)

    retryable_status_codes = {409, 429} | set(range(500, 600))  # HTTP status codes to retry

    for attempt in range(max_retries):
        try:
            # Send POST request
            response = requests.post(url, headers=headers, data=_payload)
            if response:
                response_message = response.text
                response_message = re.sub(r' +', ' ', re.sub(r'[\r\n]+', '', response_message).strip())
            else:
                if response.status_code == 401:
                    response_message = f"Authentication Error status code: {response.status_code} - requests.post({url}, headers=headers, data=_payload)"
                else:
                    response_message = f"Failure request status code: {response.status_code} - requests.post({url}, headers=headers, data={_payload})"

            # Check if response status code requires a retry
            if response.status_code in retryable_status_codes:
                if attempt < max_retries - 1:  # If not the last attempt, retry
                    sleep_time = retry_delays[attempt]
                    print(f"Attempt {attempt + 1}/{max_retries}: Received HTTP {response.status_code} "
                          f"for URL {url}. Retrying after {sleep_time} seconds... Response: {response_message}")
                    time.sleep(sleep_time)
                    continue
                else:
                    print(f"Attempt {attempt + 1}/{max_retries}: Received HTTP {response.status_code} "
                          f"for URL {url}. No more retries left. Response: {response_message}")
                    return response  # Return the response even if it's an error
            if response.status_code == 200:
                return response
            else:
                raise WorkflowError(response_message)

        except requests.RequestException as e:
            if attempt < max_retries - 1:  # If not the last attempt, retry
                sleep_time = retry_delays[attempt]
                print(f"Attempt {attempt + 1}/{max_retries}: Request failed with {e} "
                      f"for URL {url}. Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
            else:
                print(f"Attempt {attempt + 1}/{max_retries}: Request failed with {e} "
                      f"for URL {url}. No more retries left.")
                raise WorkflowError(f"No More Retries left for API call for {e}")
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Unexpected error {type(e).__name__}: {e} "
                    f"for URL {url}. No more retries left.")
            raise WorkflowError(f"No More Retries left for API call for {e}")
    return None


def get_basic_auth(_q_username, _q_password) -> str:
    authorization = 'Basic ' + \
                    base64.b64encode(f"{_q_username}:{_q_password}".encode('utf-8')).decode('utf-8')
    return authorization

#
# BEGIN MAIN
#

def configuration():
    global q_csv_file, q_database_file, q_api_fqdn, q_username, q_password, q_api_function, q_max_asset_ids, dry_run_flag

    try:
        # Get configuration
        q_csv_file, q_database_file, q_api_fqdn, q_username, q_password, q_api_function, dry_run_flag = get_config()
    except ValueError as e:
        # Errors are already printed by get_config with usage, so just exit
        sys.exit(1)


def process_workflow():

    with q_log_file.open('a', encoding='utf-8') as f:
        with redirect_stdout(f), redirect_stderr(f):
            print(f"\n=== Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
            # Workflow with configured paths and API settings
            workflow = [
                (create_axonius_table, {"_csv_data_file": q_csv_file}),
                (create_payloads_table, {"_db_path": q_database_file}),
                (populate_payloads_table, {"_db_path": q_database_file}),
                (create_payloads_duplicates_table, {"_db_path": q_database_file}),
                (create_non_duplicate_payload_table, {"_db_path": q_database_file,
                                                      "main_table": "qualys_attribute_payloads",
                                                      "dup_table": "qualys_attribute_payloads_duplicates",
                                                      "new_table": "qualys_attribute_payloads_clean",
                                                      "case_insensitive": True}),
                (create_group_payloads_by_asset_table, {"_db_path": q_database_file}),
                (create_split_payloads_table, {"_db_path": q_database_file, "max_asset_ids": q_max_asset_ids}),
                (create_transform_payloads_table, {"_db_path": q_database_file}),
                (execute_api_calls_into_execution_log,
                 {"_db_path": q_database_file,
                  "_q_api_function": q_api_function,
                  "_q_username": q_username,
                  "_q_password": q_password,
                  "_q_api_fqdn": q_api_fqdn,
                  "_q_api_endpoint": q_api_endpoint,
                  "_dry_run": dry_run_flag,
                  }
                 ),
            ]

            # Execute workflow
            for func, kwargs in workflow:
                try:
                    func(**kwargs)
                except Exception as e:
                    print(f"Error in {func.__name__}: {e}")
                    raise WorkflowError(f"Failed in {func.__name__}: {e}") from e


def main():
    global q_csv_file, q_database_file, q_api_fqdn, q_username, q_password, q_api_function, q_max_asset_ids

    print(f"\n=== Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    print(f"===See Run Log for Progress at: {q_log_file} ===")

    try:
        process_workflow()
    except WorkflowError as e:
        print(f"Workflow failed: {e}")
        print(f"===See Run Log results at: {q_log_file}  ===")
        print(f"===Review database for errors at: {q_database_file}  ===")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error in main: {e}")
        print(f"===See Run Log results at: {q_log_file}  ===")
        print(f"===Review database for errors at: {q_database_file}  ===")
        sys.exit(1)

    print(f"\n=== Run ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

def heartbeat():
    while True:
        time.sleep(15)
        print(f"custom_attributes_connector - {datetime.now():%Y-%m-%d %H:%M:%S} – still processing...", flush=True)


if __name__ == "__main__":

    check_required_modules()
    configuration()
    main()
    print(f"Results:")
    print(f"    Input CSV File:       {q_csv_file}")
    print(f"    Output Database file: {q_database_file}")
    print(f"    Log file:             {q_log_file}")



