from csv import DictReader

# https://github.com/mkleehammer/pyodbc/wiki
import pyodbc
import creds
from pprint import pprint
import xml.etree.ElementTree as ET
import requests


# from xml.etree.ElementTree import Element, SubElement, Comment, tostring


# -----------------------------
def convert_update_csv(csv):
    with open("Bulk_Profile_Test_File.csv", encoding='windows-1252') as f:
        return list(DictReader(f))


# -----------------------------
def retrieve_user_record_ids(ud):

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

    # Quick function for adding xml subnodes.
    # Subnodes require at least a parent and a tag.
    def add_subnode(parent, tag, n, o, t):
        ET.SubElement(parent, tag, name=n, operation=o).text = t

    # Main XML function
    for row in ud:

        # We need the record ID to update the record.
        # TK TK -- new hires may not have records? Discuss.
        if "user_record_id" not in row.keys(): continue

        # XML root
        root = ET.Element('update-record')
        root.set("xmlns", "http://www.symplectic.co.uk/publications/api")

        # Fields subnode
        fields = ET.SubElement(root, "fields")

        # The column names in the CSV to update for users
        update_fields = [
            'overview',
            'research-interests',
            'teaching-summary'
        ]

        for field_name in update_fields:
            if row[field_name] != "":
                add_subnode(fields, "field", field_name, "set", row[field_name])

        # Convert XML object to string.
        row["xml"] = ET.tostring(root)

    return ud


# ==========================
# MAIN PROGRAM

# TK eventually set with args
ssh_tunnel_needed = True

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
updates_dict = retrieve_user_record_ids(updates_dict)

# Adds the xml bodies for the update procedure
update_dict = create_xml_bodies(updates_dict)

pprint(update_dict)

## TK TK pick up here -- loop through dicts, send the API query with xml bodies.


if ssh_tunnel_needed:
    server.stop()