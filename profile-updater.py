from csv import DictReader

# https://github.com/mkleehammer/pyodbc/wiki
import pyodbc
import creds
from pprint import pprint
import xml.etree.ElementTree as ET
import requests
from time import sleep


# -----------------------------
def convert_update_csv(csv):
    print("Converting CSV to python dict.")
    with open("Bulk_Profile_Test_File.csv", encoding='windows-1252') as f:
        return list(DictReader(f))


# -----------------------------
def retrieve_user_record_ids(ud):
    print("Retrieving user record IDs from the Reporting DB.")

    # Load SQL creds and driver
    if ssh_tunnel_needed:
        sql_creds = creds.sql_creds_local
        sql_driver = '{ODBC Driver 18 for SQL Server}'
    else:
        sql_creds = creds.sql_creds_server
        sql_driver = '{ODBC Driver 17 for SQL Server}'

    # Open the SQL file
    try:
        sql_file = open("user_records_query.sql")
        sql_query = sql_file.read()
    except:
        raise Exception(
            "ERROR WHILE HANDLING SQL FILE. The file was unable to be located, or a problem occurred while reading its "
            "contents.")

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
        raise Exception("ERROR CONNECTING TO DATABASE. Check credits and/or SSH tunneling.")

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

    for ud_row in ud:
        ud_row["user_record_id"] = prop_id_keys_dicts[ud_row["user_proprietary_id"]]["User Record ID"]
        ud_row["user_id"] = prop_id_keys_dicts[ud_row["user_proprietary_id"]]["User ID"]

    cursor.close()
    conn.close()

    return ud


# -----------------------------
def create_xml_bodies(ud):
    print("Creating XML bodies for user record updates via API.")

    # Quick function for adding xml subnodes.
    # Subnodes require at least a parent and a tag.
    def add_subnode(parent, tag, n, o, t):
        ET.SubElement(parent, tag, name=n, operation=o).text = t

    # Main XML function
    for row in ud:

        # We need the record ID to update the record.
        # TK TK -- new hires may not have records? And may need PUT rather than PATCH? Discuss.
        if "user_record_id" not in row.keys(): continue

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
            if row[field_name] != "":
                # Params: parent, tag name, name attribute, operation attribute, text content.
                add_subnode(fields, "field", field_name, "set", row[field_name])

        # Convert XML object to string.
        row["xml"] = ET.tostring(root)

    return ud


# -----------------------------

test_records = [
    {
        'mobile-phone': '',
        'mobile-phone-ext': '',
        'overview': 'OVERVIEW CHANGED!!!',
        'personal-email': '',
        'research-interests': 'RESEARCH INTERESTS CHANGED!!!',
        'teaching-summary': '',
        'user_id': 279757,
        'user_proprietary_id': 'devin.smith@ucop.edu',
        'user_record_id': 'F66983DD-9932-452C-8409-746A939922E4',
        'work-email': 'devin,smith@ucop.edu',
        'work-phone-ext': '',
        'xml': """<update-record xmlns="http://www.symplectic.co.uk/publications/api">
	                <fields>
		                <field name="overview" operation="set">
			                <text>Updated from Python --> API</text>
		                </field>
		                <field name="research-interests" operation="set">
			                <text>Also Updated from Python --> yah yah yah changed</text>
		                </field>
	            </fields>
            </update-record>"""
    }
]


def update_records_via_api(ud):
    print("Sending update requests to API.")

    # Load creds
    api_creds = creds.api_creds

    # Loop the user update dicts
    for user_dict in test_records:

        # Append the user record URL to the endpoint
        req_url = api_creds['endpoint'] + "user/records/manual/" + user_dict['user_record_id']

        # Content type header is required when sending XML to Elements' API.
        headers = {'Content-Type': 'text/xml'}

        # Send the http request
        response = requests.patch(req_url,
                                  headers=headers,
                                  data=user_dict['xml'],
                                  auth=(api_creds['username'], api_creds['password']))

        # If something went wrong, print the details.
        if response.status_code != 200:
            print("\nNon-200 status code received:")
            pprint(response.status_code)
            pprint(response.headers['content-type'])
            pprint(response.text)

        # Half-second throttle to keep the API happy
        sleep(0.5)


# ========================================
# MAIN PROGRAM

# TK eventually set with args
ssh_tunnel_needed = True

# Open SSH tunnel if needed
if ssh_tunnel_needed:
    from sshtunnel import SSHTunnelForwarder
    server = SSHTunnelForwarder(
        creds.ssh_creds['host'],
        ssh_username=creds.ssh_creds['username'],
        ssh_password=creds.ssh_creds['password'],
        remote_bind_address=creds.ssh_creds['remote'],
        local_bind_address=creds.ssh_creds['local'])

    server.start()

# Convert the CSV to dict.
updates_dict = convert_update_csv("Bulk_Profile_Test_File.csv")

# Reporting DB: Add the user record IDs to the dict
# updates_dict = retrieve_user_record_ids(updates_dict)
retrieve_user_record_ids(updates_dict)

# Adds the xml bodies for the update procedure
# update_dict = create_xml_bodies(updates_dict)
create_xml_bodies(updates_dict)

# Send updates to the API
update_records_via_api(updates_dict)

# Close SSH tunnel if needed
if ssh_tunnel_needed:
    server.stop()

print("Program complete. Exiting.")
