# This version of the Profile Importer only uses the API.

# NOTE! This program is designed for Elements API v 5.5
# Modifications may need to be made if we've updated to a newer version.

import creds

from csv import DictReader
import pyodbc
from pprint import pprint
import xml.etree.ElementTree as ET
import requests
from time import sleep


# -----------------------------
def convert_update_csv(csv):
    print("Converting CSV to python dict.")
    with open(csv, encoding='windows-1252') as f:
        return list(DictReader(f))


def get_namespace_object():
    # Namespace URLs
    atom_ns = 'http://www.w3.org/2005/Atom'
    api_ns = 'http://www.symplectic.co.uk/publications/api'

    # Namespace object
    ns = {
        'Element': atom_ns,
        'feed': atom_ns,
        'entry': atom_ns,
        'api': api_ns,
        'records': api_ns,
    }

    return ns


# -----------------------------
def retrieve_user_ids(ud):
    print("Getting User IDs...")

    # Get the namespaces
    ns = get_namespace_object()

    # Loop the user update dicts
    for index, user_dict in enumerate(ud, 0):

        # Append the user's proprietary ID URL to the endpoint
        # Send the http request
        # Load response XML into Element Tree
        req_url = api_creds['endpoint'] + "users?proprietary-id=" + user_dict['user_proprietary_id']
        response = requests.get(req_url, auth=(api_creds['username'], api_creds['password']))
        root = ET.fromstring(response.text)

        # Locate the API object with the user ID
        id_xpath = 'feed:entry/api:object'
        user_id_element = root.find(id_xpath, ns)

        # If the user doesn't have an Elements ID, warn, empty the dict, and continue
        if user_id_element is None:
            print("\nWARNING: A user in the CSV does not have a 'User ID' in Elements."
                  "This likely indicates they are a new user. They will be skipped "
                  "for now -- We believe the Elements User ID and manual record are "
                  "created either manually or when a user's HR feed entry is imported.")
            print("user_proprietary_id:", user_dict['user_proprietary_id'])
            ud[index] = None
            continue

        # Add the user ID to the dict. Note: this is a STRING.
        user_dict['user_id'] = user_id_element.attrib['id']

        # Pause for the API.
        sleep(0.25)

    # Return the filtered set
    return [user_dict for user_dict in ud if user_dict is not None]


# -----------------------------
def retrieve_user_record_ids(ud):
    print("Getting User Record IDs...")

    # Get the namespaces
    ns = get_namespace_object()

    # Loop the user update dicts
    for index, user_dict in enumerate(ud, 0):

        # Append the user ID to the endpoint URL
        # Send the http request
        # Load response XML into Element Tree
        req_url = api_creds['endpoint'] + "users/" + user_dict['user_id']
        response = requests.get(req_url, auth=(api_creds['username'], api_creds['password']))
        root = ET.fromstring(response.text)

        # Locate the URL element, get the User ID
        record_id_xpath = 'feed:entry/api:object/api:records/api:record'
        record_id_element = root.find(record_id_xpath, ns)
        record_id = record_id_element.attrib['id-at-source']

        # If the user doesn't have a manual record ID, something has gone wrong
        if record_id is None:
            print("\nWARNING: A user in the CSV does not have a 'Manual User Record' in Elements."
                  "This likely indicates they are a new user. They will be skipped "
                  "for now -- We believe the Elements User ID and manual record are "
                  "created either manually or when a user's HR feed entry is imported.")
            print("user_proprietary_id:", user_dict['user_proprietary_id'])
            ud[index] = None
            continue

        # Add the record id to the user dict
        user_dict['user_record_id'] = record_id

        # Pause for the API.
        sleep(0.25)

    # Return the filtered set
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
def update_records_via_api(ud):
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


# ========================================
# MAIN PROGRAM

# TK eventually set with args
qa_mode = True
ssh_tunnel_needed = True

# Loads creds based on the above flags
# --------- QA
if qa_mode:
    ssh_creds = creds.ssh_creds_qa
    api_creds = creds.api_creds_qa
    if ssh_tunnel_needed:
        sql_creds = creds.sql_creds_local_qa
        sql_driver = '{ODBC Driver 18 for SQL Server}'
    else:
        sql_creds = creds.sql_creds_server_qa
        sql_driver = '{ODBC Driver 17 for SQL Server}'

# --------- PROD
else:
    ssh_creds = creds.ssh_creds
    api_creds = creds.api_creds
    if ssh_tunnel_needed:
        sql_creds = creds.sql_creds_local
        sql_driver = '{ODBC Driver 18 for SQL Server}'
    else:
        sql_creds = creds.sql_creds_server
        sql_driver = '{ODBC Driver 17 for SQL Server}'

# Open SSH tunnel if needed
if ssh_tunnel_needed:
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
updates_dict = convert_update_csv("Bulk_Profile_Test_2.csv")

# Loop the dict, and ping the APIs for user IDs and user record IDs
# These functions also strip any users who don't have these values
# in the API. (Likely means they're new users.)
updates_dict = retrieve_user_ids(updates_dict)
updates_dict = retrieve_user_record_ids(updates_dict)

# Adds the xml bodies for the update procedure
create_xml_bodies(updates_dict)

# Send updates to the API
update_records_via_api(updates_dict)

# Close SSH tunnel if needed
if ssh_tunnel_needed:
    server.stop()

print("\nProgram complete. Exiting.")
