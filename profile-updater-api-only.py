# This version of the Profile Importer only uses the API.
# This program is designed for Elements API v 5.5
# Modifications may need to be made if we've updated to a newer version.

# Libraries
import argparse
from csv import DictReader
from pprint import pprint
from time import sleep
import xml.etree.ElementTree as ET
import requests

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
# MAIN PROGRAM
def main():

    # Validate args
    if (args.csv_file is not None) and (args.connection == 'qa' or args.connection == 'production'):
        pass
    else:
        print("Invalid arguments provided. See here:")
        print(parser.print_help())
        exit(0)

    # Load creds based on the above flags
    if args.connection == 'qa':
        ssh_creds = creds.ssh_creds_qa
        api_creds = creds.api_creds_qa
    else:
        ssh_creds = creds.ssh_creds_prod
        api_creds = creds.api_creds_prod

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
    update_dicts = convert_update_csv(args.csv_file)

    # Loop the dict, and ping the APIs for user IDs and user record IDs
    # This function also removes users who don't have these values in the API.
    update_dicts = retrieve_user_record_ids(update_dicts, api_creds)

    # The columns in the CSV to update for users
    update_fields = [
        'overview',
        'research-interests',
        'teaching-summary'
    ]

    # Adds the xml bodies for the update procedure
    create_xml_bodies(update_dicts, update_fields)

    # Send updates to the API
    update_records_via_api(update_dicts, api_creds)

    # Close SSH tunnel if needed
    if args.tunnel_needed:
        server.stop()

    print("\nProgram complete. Exiting.")


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
def retrieve_user_record_ids(ud, api_creds):
    print("Getting User Record IDs...")

    # ----- Error text if record(s) not found.
    def print_error(prop_id):
        print("\nWARNING: A user in the CSV does not have a User ID or manual user record in Elements."
              "This likely indicates they are a new user. They will be skipped for now -- "
              "We believe the Elements User ID and manual record are created "
              "either manually or when a user's HR feed entry is imported.")
        print("   user_proprietary_id:", prop_id)

    # Namespace object for xpath
    ns = {
        'feed': 'http://www.w3.org/2005/Atom',
        'api': 'http://www.symplectic.co.uk/publications/api'
    }

    # Loop the user update dicts
    for index, user_dict in enumerate(ud, 0):

        # Append the user ID to the endpoint URL and send the http request
        req_url = api_creds['endpoint'] + "users/pid-" + user_dict['user_proprietary_id']
        response = requests.get(req_url, auth=(api_creds['username'], api_creds['password']))

        # Skip if the user doesn't have a manual record ID
        if response.status_code != 200:
            print_error(user_dict['user_proprietary_id'])
            ud[index] = None
            continue

        # Load response XML into Element Tree
        root = ET.fromstring(response.text)

        # Locate the 'native' aka 'manual' user record element
        record_id_xpath = 'feed:entry/api:object/api:records/api:record[@format="native"]'
        record_id_element = root.find(record_id_xpath, ns)

        # Skip if no "native" records are found
        if record_id_element is None:
            print_error(user_dict['user_proprietary_id'])
            ud[index] = None
            continue

        # Add the record id to the user dict
        user_dict['user_record_id'] = record_id_element.attrib['id-at-source']

        # Throttle API calls
        sleep(0.5)

    # Return the filtered set
    return [user_dict for user_dict in ud if user_dict is not None]


# -----------------------------
def create_xml_bodies(ud, update_fields):

    # NOTE -- Body XML hierarchy:
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

    print("Creating XML bodies for user record updates via API.")

    # Main XML function
    for user_dict in ud:

        # XML root <update-record> and child node <fields>
        root = ET.Element('update-record', xmlns="http://www.symplectic.co.uk/publications/api")
        fields = ET.SubElement(root, "fields")

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
            print("   User Proprietary ID:", user_dict['user_proprietary_id'])
            print("   User Record ID:", user_dict['user_record_id'])

        else:
            print("\nNon-200 status code received:")
            pprint(response.status_code)
            pprint(response.headers['content-type'])
            pprint(response.text)

        # Throttle API calls
        sleep(0.5)


# -----------------------------
# Stub for main
if __name__ == "__main__":
    main()
