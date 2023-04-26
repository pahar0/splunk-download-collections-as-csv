import requests
from requests.auth import HTTPBasicAuth
import csv
import os
import getpass
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

def main(app_name, kvstore_collection, splunk_username, splunk_password):
    print("Processing KV store data...")

    # Remove "kvstore_" prefix if it exists
    if kvstore_collection.startswith("kvstore_"):
        kvstore_collection = kvstore_collection[len("kvstore_"):]

    # Replace these with your Splunk credentials and instance details
    splunk_base_url = 'https://127.0.0.1:8389'

    # Create backup and app_name directories if they don't exist
    os.makedirs(f'backup/{app_name}', exist_ok=True)
    print(f"Created/checked backup directory: backup/{app_name}")

    # Define the output CSV file
    output_csv = f'backup/{app_name}/kvstore_{kvstore_collection}.csv'

    # Authenticate and get the kvstore data
    auth = HTTPBasicAuth(splunk_username, splunk_password)
    url = f'{splunk_base_url}/servicesNS/nobody/{app_name}/storage/collections/data/{kvstore_collection}'

    params = {
        'output_mode': 'json'
    }

    print("Requesting KV store data from Splunk...")
    response = requests.get(url, auth=auth, params=params, verify=False)

    print("Status Code:", response.status_code)

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print("JSONDecodeError: No JSON content in the response")
        else:
            print("Writing data to the CSV file...")
            # Write data to CSV file
            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                if data:
                    fieldnames = [key for key in data[0].keys() if key not in ['_user', '_key']]
                    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
                    csv_writer.writeheader()

                    for row in data:
                        # Filter the row dictionary to only include fields from fieldnames
                        filtered_row = {key: row[key] for key in fieldnames if key in row}
                        csv_writer.writerow(filtered_row)

                    print("Backup successful!")
                else:
                    print("No data found in the kvstore collection.")
    else:
        print("Failed to fetch data from the KV store.")

if __name__ == '__main__':
    print("Please enter the following information:")
    app_name = input("Splunk app name: ")
    kvstore_collection = input("KV store collection name: ")
    splunk_username = input("Splunk username: ")
    splunk_password = getpass.getpass("Splunk password: ")

    main(app_name, kvstore_collection, splunk_username, splunk_password)
