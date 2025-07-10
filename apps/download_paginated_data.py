#!/usr/bin/env python3
import os
import requests
import re
from datetime import date

# The Socrata API endpoint for the 311 dataset.
# Note: we request .csv format directly.
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

# The SoQL query to select data.
# WHERE will be added inside the loop.
SOQL_QUERY_SELECT = """
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
""".replace('\n', ' ').strip()
# Replace multiple spaces with a single space to form a valid SoQL query
SOQL_QUERY_SELECT = re.sub(r'\s+', ' ', SOQL_QUERY_SELECT)

# --- Configuration ---
# This script assumes it is run from the root of the project directory.
OUTPUT_DIR = "data/input"
# Name of the output file pattern.
OUTPUT_FILE_PATTERN = "311_service_requests_{year}_{month:02d}.csv"
# Socrata APIs have a limit on how many records can be returned at once.
# We set a high limit to try and get all records for a given month.
# This may need adjustment if a month has more records than this limit.
LIMIT = 500000

def download_data_by_month():
    """
    Downloads 311 data from the Socrata API for each month of each year
    and saves it to a single CSV file per month.
    """
    # Ensure the output directory exists.
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Starting download. Data will be saved to {OUTPUT_DIR}/")

    current_year = date.today().year
    # Loop from 2010 to the current year.
    for year in range(2023, current_year + 1):
        # Loop through each month of the year.
        for month in range(1, 13):
            # Stop if we are trying to fetch data for a future month.
            if year == current_year and month > date.today().month:
                break

            output_filename = OUTPUT_FILE_PATTERN.format(year=year, month=month)
            output_path = os.path.join(OUTPUT_DIR, output_filename)

            # Skip if file already exists to make the script idempotent.
            if os.path.exists(output_path):
                print(f"File {output_path} already exists. Skipping.")
                continue

            print(f"Fetching data for {year}-{month:02d}...")

            # Construct the full SoQL query with year and month filter.
            soql_query = (
                f"{SOQL_QUERY_SELECT} "
                f"WHERE date_extract_y(created_date) = {year} "
                f"AND date_extract_m(created_date) = {month} "
                f"ORDER BY created_date DESC "
                f"LIMIT {LIMIT}"
            )

            params = {
                '$query': soql_query
            }

            try:
                # Use a longer timeout for potentially large monthly datasets
                response = requests.get(BASE_URL, params=params, timeout=300)
                response.raise_for_status()

                # A response with only a header row means no data was found.
                if len(response.text.strip().split('\n')) <= 1:
                    print(f"No data found for {year}-{month:02d}.")
                    continue

                print(f"Saving data to {output_path}")
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)

            except requests.exceptions.RequestException as e:
                print(f"An error occurred for {year}-{month:02d}: {e}")
                continue

    print("\nDownload complete.")

if __name__ == "__main__":
    download_data_by_month()
