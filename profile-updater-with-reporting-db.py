# This version of the profile importer uses both the API and Reporting DB
# NOTE! This program is designed for Elements API v 5.5
# Modifications may need to be made if we've updated to a newer version.

import argparse
from csv import DictReader
import pyodbc
from pprint import pprint
import xml.etree.ElementTree as ET
import requests
from time import sleep

# Creds file
# This script requires a "creds.py" in its directory.
# See "creds_template.py" for the required format.
import creds

# -----------------------------
# Arguments
parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input",
                    dest="csv_file",
                    type=str,
                    help="REQUIRED. The CSV file to process.")

parser.add_argument("-c", "--connection",
                    dest="connection",
                    type=str.lower,
                    help="REQUIRED. Specify ONLY 'qa' or 'production'")

parser.add_argument("-t", "--tunnel",
                    dest="tunnel_needed",
                    action="store_true",
                    default=False,
                    help="Optional. Include to run the connection through a tunnel.")

args = parser.parse_args()


# ========================================
def main():

    # Validate args
    if (args.csv_file is not None) and (args.connection == 'qa' or args.connection == 'production'):
        pass
    else:
        print("Invalid arguments provided. See here:")
        print(parser.print_help())
        exit(0)

    # Loads creds based on the above flags
    # --------- QA
    if args.connection == 'qa':
        ssh_creds = creds.ssh_creds_qa
        api_creds = creds.api_creds_qa
        if args.tunnel_needed:
            sql_creds = creds.sql_creds_local_qa
            sql_driver = '{ODBC Driver 18 for SQL Server}'
        else:
            sql_creds = creds.sql_creds_server_qa
            sql_driver = '{ODBC Driver 17 for SQL Server}'

    # --------- PROD
    else:
        ssh_creds = creds.ssh_creds_prod
        api_creds = creds.api_creds_prod
        if args.tunnel_needed:
            sql_creds = creds.sql_creds_local_prod
            sql_driver = '{ODBC Driver 18 for SQL Server}'
        else:
            sql_creds = creds.sql_creds_server_prod
            sql_driver = '{ODBC Driver 17 for SQL Server}'

    # Open SSH tunnel if needed
    if args.tunnel_needed:
        print("Opening SSH tunnel.")
        from sshtunnel import SSHTunnelForwarder

        server = SSHTunnelForwarder(
            ssh_creds['host'],
            ssh_username=ssh_creds['username'],
            ssh_password=ssh_creds['password'],
            remote_bind_address=ssh_creds['remote'],
            local_bind_address=ssh_creds['local'])

        server.start()

    # Convert the CSV to dict.
    updates_dict = convert_update_csv(args.csv_file)

    # Reporting DB: Add the user record IDs to the dict,
    # Also filters out any users not yet in the system
    updates_dict = retrieve_user_record_ids(updates_dict, sql_creds, sql_driver)

    # Adds the xml bodies for the update procedure
    create_xml_bodies(updates_dict)

    # Send updates to the API
    update_records_via_api(updates_dict, api_creds)

    # Close SSH tunnel if needed
    if args.tunnel_needed:
        server.stop()

    print("Program complete. Exiting.")

# ========================================


# -----------------------------
def convert_update_csv(csv):
    print("Converting CSV to a python list of dicts.")

    try:
        with open(csv, encoding='windows-1252') as f:
            return list(DictReader(f))

    except:
        print("\nAn error occurred while loading the specified -i --input file.\n\n"
              "TROUBLESHOOTING: This .py script specifies the CSV encoding in "
              "the function 'convert_update_csv'. An encoding mismatch will "
              "trigger this error. Make sure the encodings match.")
        exit(0)


# -----------------------------
def retrieve_user_record_ids(ud, sql_creds, sql_driver):
    print("Retrieving user record IDs from the Reporting DB.")

    # Open the SQL file
    try:
        sql_file = open("user_records_query.sql")
        sql_query = sql_file.read()
    except:
        raise Exception(
            "ERROR WHILE LOADING/READING THE SQL FILE.")

    # Create the list of proprietary IDs
    prop_id_list = ",\n".join(["'" + item['user_proprietary_id'] + "'" for item in ud])
    sql_query = sql_query.replace("-- REPLACE", prop_id_list)

    # Connect to Elements Reporting DB
    try:
        conn = pyodbc.connect(
            driver=sql_driver,
            server=(sql_creds['server'] + ',' + sql_creds['port']),
            database=sql_creds['database'],
            uid=sql_creds['user'],
            pwd=sql_creds['password'],
            trustservercertificate='yes')
    except:
        raise Exception("ERROR CONNECTING TO DATABASE.")

    # Create cursor, execute query
    cursor = conn.cursor()
    cursor.execute(sql_query)

    # pyodbc doesn't have a dict-cursor option, have to do it manually
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Re-format the dict with user Proprietary IDs as keys
    prop_id_keys_dicts = {item["Proprietary ID"]:
                              {'User Record ID': item["Data Source Proprietary ID"],
                               "User ID": item["User ID"]
                               } for item in rows}

    # Loop through the user dicts.
    # If there's no matching "Proprietary ID" in the SQL results, set to None (filters during the return)
    # Otherwise, add the SQL results to the dict.
    for index, user_dict in enumerate(ud, 0):
        if user_dict["user_proprietary_id"] not in prop_id_keys_dicts.keys():
            print("\nWARNING: A user in the CSV does not have a manual 'User Record' in the reporting DB. This likely "
                  "indicates they are a new user. They will be skipped for updating until they have a manual user "
                  "record -- which we believe occurs when they are created in Elements, either manually or via an HR "
                  "feed update.")
            print("user_proprietary_id:", user_dict['user_proprietary_id'])
            ud[index] = None

        else:
            user_dict["user_record_id"] = prop_id_keys_dicts[user_dict["user_proprietary_id"]]["User Record ID"]
            user_dict["user_id"] = prop_id_keys_dicts[user_dict["user_proprietary_id"]]["User ID"]

    cursor.close()
    conn.close()

    return [user_dict for user_dict in ud if user_dict is not None]


# -----------------------------
def create_xml_bodies(ud):
    print("Creating XML bodies for user record updates via API.")

    # Body XML format hierarchy should be:
    #
    # <update-record>
    #   <fields>
    #       <field>
    #           <text>abc</text>
    #       </field>
    #       <field>
    #           <text>xyz</text>
    #       </field>
    #   </fields>
    # </update-record>

    # Main XML function
    for user_dict in ud:
        # if "user_record_id" not in user_dict.keys(): continue

        # XML root <update-record> and child node <fields>
        root = ET.Element('update-record', xmlns="http://www.symplectic.co.uk/publications/api")
        fields = ET.SubElement(root, "fields")

        # The columns in the CSV to update for users
        update_fields = [
            'overview',
            'research-interests',
            'teaching-summary'
        ]

        # Create an XML node for each of the user's non-empty fields
        for field_name in update_fields:
            if user_dict[field_name] != "":
                field = ET.SubElement(fields, "field", name=field_name, operation="set")
                ET.SubElement(field, "text").text = user_dict[field_name]

        # Convert XML object to string.
        user_dict["xml"] = ET.tostring(root)


# -----------------------------
def update_records_via_api(ud, api_creds):
    print("Sending update requests to API...")

    # Loop the user update dicts
    for user_dict in ud:

        # Append the user record URL to the endpoint
        req_url = api_creds['endpoint'] + "user/records/manual/" + user_dict['user_record_id']

        # Content type header is required when sending XML to Elements' API.
        headers = {'Content-Type': 'text/xml'}

        # Send the http request
        response = requests.patch(req_url,
                                  headers=headers,
                                  data=user_dict['xml'],
                                  auth=(api_creds['username'], api_creds['password']))

        # Report on updates
        if response.status_code == 200:
            print("\nSuccessful update: ")
            print("  User ID:", user_dict['user_id'])
            print("  User Prop. ID:", user_dict['user_proprietary_id'])
            print("  User Record ID:", user_dict['user_record_id'])

        else:
            print("\nNon-200 status code received:")
            pprint(response.status_code)
            pprint(response.headers['content-type'])
            pprint(response.text)

        # Half-second throttle to keep the API happy
        sleep(0.5)


# -----------------------------
# Stub for main
if __name__ == "__main__":
    main()
