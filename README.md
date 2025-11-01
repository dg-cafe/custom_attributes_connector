# Custom Attributes Connector

This Python application processes a CSV file exported from Axonius, cleans and transforms the data, stores it in an SQLite database, generates JSON payloads for updating custom attributes on Qualys assets, and executes API calls to the Qualys platform to add, update, or remove these attributes. It handles data deduplication, grouping, splitting large batches, and retries on API errors. The application supports a dry-run mode to prototype operations in database tables without making actual API calls.

The script is designed for integrating Axonius data with Qualys, specifically for managing custom attributes on assets identified by Qualys IDs.

## Workflow
The custom attributes connector workflow is depicted to demonstrate stages of transformation stored in local database for post process analysis and reporting of success/fail.

[![](https://github.com/user-attachments/assets/ca05ef62-e14b-4ae0-9475-d268e401bd32)](https://github.com/user-attachments/assets/ca05ef62-e14b-4ae0-9475-d268e401bd32)


## Requirements

- **Python Version**: Python 3.6+ (tested on 3.12.3, but compatible with earlier versions).
- **Required Modules**:
  - Standard Library: `csv`, `sqlite3`, `sys`, `io`, `re`, `pathlib`, `argparse`, `os`, `requests`, `json`, `base64`, `datetime`, `time`, `contextlib`, `concurrent.futures`.
  - Third-Party: `requests` (for API calls).
- **Installation**: Install missing modules via `pip install requests`.
- **No Internet Access for Code Execution**: The script does not require installing additional packages at runtime; all dependencies must be pre-installed.
- **Qualys API Access**: Requires a Qualys account with API permissions for asset management.

## Installation

1. Clone the repository:
    ```
   # Change to your working directory where the application will run.
   git clone https://github.com/dg-cafe/custom_attributes_connector
   cd custom_attributes_connector

    ```

    ```
    # Optional, if you just want to get the custom_attributes_connector.py
    curl https://raw.githubusercontent.com/dg-cafe/custom_attributes_connector/refs/heads/main/custom_attributes_connector.py > custom_attributes_connector.py

    ```

1. Install dependencies:
   ```
   pip install requests
   
   ```


### Place your Axonius-exported CSV file in `./data/input.csv` (or specify via arguments).

## Usage

Run the script from the command line, providing options or environment variables for configuration.

```bash
# python3 custom_attributes_connector.py [options]

```

```bash
# Example 1) Linux/Mac Dry Run to create database for validation/review:

export q_username=[your qualys userid]
export q_password=[your qualys password]
python3 custom_attributes_connector.py -c input.csv -a qualysapi.qg3.apps.qualys.com -f add --dry-run

```

```bash
# Example 2) Linux/Mac Execution of API Calls

export q_username=[your qualys userid]
export q_password=[your qualys password]
python3 custom_attributes_connector.py -c input.csv -a qualysapi.qg3.apps.qualys.com -f add

```

```bash
# Example 3) Linux/Mac Print Help Screen.

python3 custom_attributes_connector.py -h

```

### Options
- `-c, --csv-file PATH`: Path to the input CSV file (required, must exist, default: `./data/input.csv`).
- `-d, --dry-run`: Do not execute any API calls.
- `--db-help`: Show database table descriptions and exit.
- `-a, --api-fqdn FQDN`: Qualys API fully qualified domain name (default: `qualysapi.qg3.apps.qualys.com`).
- `-f, --api-function FUNC`: Qualys API function to perform on assets custom attributes: `add`, `update`, or `remove` (default: `add`).
  - `add`: Add custom attribute key/data pair from CSV if the key does not exist.
  - `update`: Update custom attribute key/data pair if the key exists.
  - `remove`: Remove custom attribute key/data pair if the key exists.
- `-h, --help`: Show this help message and exit.

### Environment Variables
- `q_csv_file`: Alternative to `--csv-file` (must point to an existing file).
- `q_api_fqdn`: Alternative to `--api-fqdn` (default: `qualysapi.qg3.apps.qualys.com`).
- `q_api_function`: Alternative to `--api-function` (default: `add`, must be `add`, `update`, or `remove`).
- `q_username`: Qualys API user ID (required).
- `q_password`: Qualys API password (required).

### Examples

  ####
  #### Note you may have python or python3 depending on your installation.
  #### 

- **Linux (Bash)**:
  ```bash
    export q_username=your_username 
    export q_password=your_password 
    python3 custom_attributes_connector.py --csv-file input.csv --api-fqdn qualysapi.qg3.apps.qualys.com -api-function add
  ```

- **Command-line arguments**:
  ```bash
  python3 custom_attributes_connector.py --csv-file ./data/input.csv --api-fqdn qualysapi.qg3.apps.qualys.com --api-function update
  ```

**Note**: You must set `q_username` to your Qualys API user ID and `q_password` to your Qualys API password as environment variables.

## CSV Data Contract

The input CSV file must adhere to a specific contract. Each value is cleaned during transformation:

1. Newlines are replaced with commas.
2. Trailing or leading whitespace characters are removed.
3. Byte Order Mark (BOM) characters are removed (`\ufeff`).
4. Records with multiple asset IDs result in a separate record for each asset ID.

The CSV must contain the following fields, mapped to Qualys custom attribute keys:

| CSV Header                      | Mapped Key          |
|---------------------------------|---------------------|
| Qualys Scans: Qualys ID         | AssetID             |
| Mssql: MSSQL_ Business_Name     | Business            |
| Mssql: MSSQL_ Div_Name          | Division            |
| Mssql: MSSQL_ Productgroup      | Product_Group       |
| Mssql: MSSQL_ Portfolio         | Portfolio           |
| Mssql: MSSQL_ Product           | Product             |
| Mssql: MSSQL_ Primaryservice    | Primary_Service     |
| Mssql: MSSQL_ Serviceownergroup | Service_Owner_Group |
| Mssql: MSSQL_ Deviceownergroup  | Device_Owner_Group  |
| Mssql: MSSQL_ Recoverytier      | Recovery_Tier       |
| Mssql: MSSQL_ Sla               | SLA                 |

## Operation Workflow

The application follows a sequential workflow:

1. **Configuration**: Parses command-line arguments and environment variables for CSV path, database path, API FQDN, function (add/update/remove), username, password, and dry-run flag.
2. **CSV Processing**: Reads the CSV row-by-row, cleans data, splits multi-asset ID rows, and inserts into the `axonious_data` table.
3. **Payload Generation**: Creates initial payloads for each asset, stores in `qualys_attribute_payloads`.
4. **Deduplication**: Identifies duplicates in `qualys_attribute_payloads_duplicates` and creates a clean table `qualys_attribute_payloads_clean`.
5. **Grouping**: Groups payloads by unique custom attributes in `qualys_attribute_payloads_grouped`.
6. **Splitting**: Splits large groups (exceeding `q_max_asset_ids=100`) into batches in `qualys_attribute_payloads_split`.
7. **Transformation**: Updates payloads with exact asset IDs and attributes in `qualys_attribute_payloads_transformed`.
8. **API Execution**: If not dry-run, sends POST requests to Qualys API with retries (up to 10 attempts, with linear backoff from 30-300 seconds) for errors like 409, 429, or 5xx. Logs results in `qualys_attribute_payloads_transformed_execution_log`.

- **API Endpoint**: `https://{q_api_fqdn}/qps/rest/2.0/update/am/asset`.
- **Authentication**: Basic Auth using `q_username` and `q_password`.
- **Headers**: `X-Requested-With: custom_attributes_connector_v1.0`, `Content-Type: application/json`.
- **Retry Logic**: Handles concurrency (409), rate limiting (429), and server errors (5xx).
- **Dry Run**: Creates database tables but skips API calls.

A heartbeat message is printed every 15 seconds during processing.

## Database Files

The application generates an SQLite database file named `custom_attributes_connector_sqlite_{timestamp}.db` (e.g., `custom_attributes_connector_sqlite_20251029_120000.db`). This file contains several tables for data processing and logging.

### Database Table Descriptions

1. **axonious_data**
   - **Purpose**: Stores cleaned and transformed data from the input CSV file. Each row represents a single Qualys asset ID with associated custom attributes.
   - **Schema**:
     - `qualys_id` (TEXT)
     - `Business` (TEXT)
     - `Division` (TEXT)
     - `Product_Group` (TEXT)
     - `Portfolio` (TEXT)
     - `Product` (TEXT)
     - `Primary_Service` (TEXT)
     - `Service_Owner_Group` (TEXT)
     - `Device_Owner_Group` (TEXT)
     - `Recovery_Tier` (TEXT)
     - `SLA` (TEXT)

2. **qualys_attribute_payloads**
   - **Purpose**: Stores initial Qualys API payloads for each asset ID.
   - **Schema**:
     - `asset_id` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)

3. **qualys_attribute_payloads_duplicates**
   - **Purpose**: Identifies duplicate asset IDs for validation.
   - **Schema**:
     - `asset_id` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)

3.1 **qualys_attribute_payloads_clean**
   - **Purpose**: Deduplicated version of `qualys_attribute_payloads`.
   - **Schema**:
     - `asset_id` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)

4. **qualys_attribute_payloads_grouped**
   - **Purpose**: Groups payloads by unique custom attributes.
   - **Schema**:
     - `asset_ids` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)
     - `count_asset_ids` (INTEGER)
     - `group_number` (INTEGER)

5. **qualys_attribute_payloads_split**
   - **Purpose**: Splits large groups into batches.
   - **Schema**:
     - `asset_ids` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)
     - `count_asset_ids` (INTEGER)
     - `group_number` (INTEGER)
     - `batch_number` (INTEGER)

6. **qualys_attribute_payloads_transformed**
   - **Purpose**: Transformed payloads ready for API calls.
   - **Schema**:
     - `asset_ids` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)
     - `count_asset_ids` (INTEGER)
     - `group_number` (INTEGER)
     - `batch_number` (INTEGER)

7. **qualys_attribute_payloads_transformed_execution_log**
   - **Purpose**: Logs API call executions.
   - **Schema**:
     - `asset_ids` (TEXT)
     - `payload` (TEXT)
     - `payload_custom_attributes` (TEXT)
     - `count_asset_ids` (INTEGER)
     - `group_number` (INTEGER)
     - `batch_number` (INTEGER)
     - `status` (TEXT)
     - `execution_log` (TEXT)

To view table descriptions from the script, run: `python custom_attributes_connector.py --db-help`.

## Logging

- **Log File**: `custom_attributes_connector_log_{timestamp}.log` captures stdout/stderr.
- **API Logs**: In `execution_log` column of the final database table.

## Limitations and Notes

- **Batch Size**: Limited to 100 asset IDs per API call (configurable via `q_max_asset_ids`).
- **Error Handling**: Workflow stops on critical errors; check log and database.
- **Security**: Use environment variables for credentials.
- **Version**: v1.0 (based on `X-Requested-With` header).

## License

[Apache](LICENSE) .
```
