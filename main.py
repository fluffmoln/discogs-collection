import requests
import psycopg2
import time
import json

# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'fluffmoln',
    'password': 'krusty1!BURGER',
}

# Replace 'YOUR_API_KEY' and 'YOUR_USER_AGENT' with your Discogs API key and a user-agent string
API_KEY = 'tvagGfDrinEkSmQHDbrDoZbvKKhVDZUVCPvwdMzM'
USER_AGENT = 'fluffmoln-discogs-collection'

# Replace 'YOUR_USERNAME' with your Discogs username
username = 'fluffmoln'

# Set up the API endpoint and headers
# endpoint_folders = 'https://api.discogs.com/users/{username}/collection/folders'
headers = {
    'User-Agent': USER_AGENT,
    'Authorization': f'Discogs token={API_KEY}'
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Table name
schema_name = 'discogs'
table_name = 'releases'

# Truncate table
cursor.execute(f"""TRUNCATE {schema_name}.{table_name};""")

# Drop and recreate table
# cursor.execute(f"""drop table if exists {schema_name}.{table_name};""")
#
# cursor.execute(f"""create table discogs.releases (
# date_added timestamp,
# artist text,
# title text,
# year integer,
# record_label text,
# catalogue_number text,
# format text,
# description text,
# media_condition text,
# sleeve_condition text,
# notes text,
# resource_url text
# );""")

# Check if the request was successful (status code 200)
# if response.status_code == 200:
#    # Parse the JSON response
#    data = response.json()


# Define function to insert data into PostgreSQL
def insert_data(release_data):
    for release in release_data['releases']:
        # Date added to collection, not necessarily date of purchase
        date_added = release.get('date_added', None)

        # Record metadata
        title = release['basic_information'].get('title', None)
        artist = release['basic_information']['artists'][0].get('name', None)
        year = release['basic_information'].get('year', None)
        resource_url = release['basic_information'].get('resource_url', None)

        # Format information
        formats = release['basic_information'].get('formats', [])
        format = formats[0].get('name', None)
        description = formats[0].get('descriptions', [None])[0]

        # Labels information
        labels = release['basic_information'].get('labels', [])
        record_label = labels[0].get('name', None)
        catalogue_number = labels[0].get('catno', None)

        # Handling notes information
        notes = release.get('notes', [])
        media_condition = notes[0].get('value', None) if notes else None
        sleeve_condition = notes[1].get('value', None) if len(notes) > 1 else None
        other_notes = [note.get('value', None) for note in notes[2:]] if len(notes) > 2 else None

        # Additional API call to get marketplace data
        marketplace_data = get_marketplace_data(resource_url)

        # Extract low, median, and high values from the marketplace data
        low_value = marketplace_data.get('lowest_price', None)
        median_value = marketplace_data.get('median_price', None)
        high_value = marketplace_data.get('highest_price', None)

        cursor.execute(f"""
            INSERT INTO {schema_name}.{table_name} (title, artist, year, resource_url, date_added, format, description, 
            record_label, catalogue_number, media_condition, sleeve_condition, notes, low_value, median_value, high_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (title, artist, year, resource_url, date_added, format, description, record_label, catalogue_number,
              media_condition, sleeve_condition, other_notes, low_value, median_value, high_value))

        conn.commit()

# Function to get marketplace data from Discogs API
def get_marketplace_data(resource_url):
    # Make an API call to the Discogs marketplace endpoint for the specific release
    url = f'{resource_url}/marketplace'
    response = requests.get(url, headers=headers)
    data = response.json()

    return data

# Paginate through Discogs API results
page = 1
while True:
    endpoint = f'https://api.discogs.com/users/{username}/collection/folders/0/releases?page={page}'
    response = requests.get(endpoint, headers=headers)
    data = response.json()

    print(f"Currently on page {page}")

    # Insert data into PostgreSQL
    insert_data(data)

    # Move to the next page
    page += 1

    if page > data['pagination']['pages']:
        break

    # Introduce a delay of 10 seconds between requests
    time.sleep(10)

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

# else:
#   print(f"Error: {response.status_code}, {response.text}")
