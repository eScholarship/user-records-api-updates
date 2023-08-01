# Elements API: User Record updater

This is a quick demo program for patching user record updates via the Elements API.

1. Depending on QA/PROD and LOCAL/SERVER, set up the creds (not on github)
2. Start SSH tunnel if needed
3. Convert the CSV into a python dict
4. Connect to the **REPORTING DB** --  With the CSV-provided proprietary IDs, get the manual User Record ID, and the User ID (in case we need this later?) These are added to the dict.
5. Loop through the dict and add the XML body for each user's API update.
6. Loop through the dict:
   1. Use the USER RECORD to access the API endpoint
   2. Send the HTTP request with the body XML
   3. If any non-200 results are returned, print the response.

**Note:**
The expected input is a CSV file with columns including:
* user_proprietary_id
* overview
* research-interests
* teaching-summary


