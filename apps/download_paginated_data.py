#!/usr/bin/env python3
import os
import requests
import glob
import re

# The Socrata API endpoint for the 311 dataset.
# Note: we request .csv format directly.
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

# The SoQL query to select data since 2020.
# This is the URL-decoded version of the query provided.
# LIMIT and OFFSET will be added inside the loop.
SOQL_QUERY_BASE = """
SELECT
    unique_key, created_date, closed_date, agency, agency_name,
    complaint_type, descriptor, location_type, incident_zip,
    incident_address, street_name, cross_street_1, cross_street_2,
    intersection_street_1, intersection_street_2, address_type, city,
    landmark, facility_type, status, due_date, resolution_description,
    resolution_action_updated_date, community_board, bbl, borough,
    x_coordinate_state_plane, y_coordinate_state_plane,
    open_data_channel_type, park_facility_name, park_borough,
    vehicle_type, taxi_company_borough, taxi_pick_up_location,
    bridge_highway_name, bridge_highway_direction, road_ramp,
    bridge_highway_segment, latitude, longitude, location
WHERE
    `created_date` > '2020-01-01' AND `created_date` IS NOT NULL
ORDER BY
    created_date DESC
""".replace('\n', ' ').strip()
# Replace multiple spaces with a single space to form a valid SoQL query
SOQL_QUERY_BASE = re.sub(r'\s+', ' ', SOQL_QUERY_BASE)

# --- Configuration ---
# This script assumes it is run from the root of the project directory.
OUTPUT_DIR = "data/raw"
# Name of the output file pattern.
OUTPUT_FILE_PATTERN = "311_service_requests_offset_{offset}.csv"
# Number of records to fetch per request.
LIMIT = 1000000
def download_data_paginated():
    """
    Downloads 311 data from the Socrata API in chunks using pagination
    and saves each chunk to a separate CSV file.
    """
    # Ensure the output directory exists.
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if files matching the pattern already exist and skip if they do.
    if glob.glob(os.path.join(OUTPUT_DIR, "311_service_requests_offset_*.csv")):
        print(f"Data files already exist in {OUTPUT_DIR}. Skipping download process.")
        return

    print(f"Starting download. Data will be saved to {OUTPUT_DIR}/")

    offset = 0
    while True:
        # Append LIMIT and OFFSET to the SoQL query string itself.
        paginated_query = f"{SOQL_QUERY_BASE} LIMIT {LIMIT} OFFSET {offset}"

        # Construct the full URL with only the $query parameter.
        params = {
            '$query': paginated_query
        }

        output_filename = OUTPUT_FILE_PATTERN.format(offset=offset)
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        print(f"Fetching {LIMIT} records from offset {offset}...")

        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes

            # For CSV, an empty response will have only the header.
            # A simple check is to see if content has more than one line.
            if len(response.text.strip().split('\n')) <= 1:
                print("No more data to download.")
                break

            print(f"Saving data to {output_path}")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

            offset += LIMIT

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    print("Download complete.")

if __name__ == "__main__":
    download_data_paginated()
