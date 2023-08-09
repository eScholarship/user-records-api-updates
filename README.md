# Elements API: User Profile Updater

This is a quick demo program which updates user records via the Elements API. There are two programs here: One uses only the API, the other uses both the API and Reporting DB. **For simplicity, start with the API-only version first.**

## Requirements:
- This program expects a file named "creds.py" with all of your login info. See the file "creds_emplate.py" for the required formatting.
- External libraries:
   - [http requests](https://requests.readthedocs.io/en/latest/),
   - (Optional) [ssh tunnel](https://pypi.org/project/sshtunnel/) 
   - (Reporting DB version only): [pyodbc](https://pypi.org/project/pyodbc/)

## Expected input
The expected input is a CSV file with columns including:
* user_proprietary_id
* overview
* research-interests
* teaching-summary

## Usage / Arguments:
profile-updater-api-only.py \[-h] \[-i CSV_FILE] \[-c CONNECTION] \[-t]

**options:**
   -h, --help
      show this help message and exit

   -i CSV_FILE, --input CSV_FILE
      REQUIRED. The CSV file to process.

   -c CONNECTION, --connection CONNECTION
      REQUIRED. Specify ONLY 'qa' or 'production'

   -t, --tunnel
      Optional. Include to run the connection through a tunnel.

## General Flow
1. Validate args and load creds
2. Start SSH tunnel if needed.
3. Convert the CSV into a python list of dicts
4. Get the "User Record IDs":
   1. For each user, ping the API, and search the XML response with xpath to locate the user record ID.
   2. If any user doesn't have matching records, print a warning, and remove them from the list.
5. Create and add body XMLs each user dict:
   1. Specify which columns on the CSV are being updated
   2. use Element Tree to construct the body XML
6. Send the updates to the API:
   1. Send the body HTML in a PATCH request to the users' 'User Record ID'
   2. If we receive any non-200 resonses, print and continue.

The **Reporting DB** version works the same way, except step 4 is replaced by an SQL query to the Reporting DB, which retrieves all of the information in a single transaction.
