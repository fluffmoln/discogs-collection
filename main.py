import requests
import psycopg2
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

# Check if the request was successful (status code 200)
# if response.status_code == 200:
#    # Parse the JSON response
#    data = response.json()

# Truncate table
cursor.execute(f"""TRUNCATE {schema_name}.{table_name};""")

# Define function to insert data into PostgreSQL
def insert_data(release_data):
    for release in release_data['releases']:
        basic_info = release.get('basic_information', {})

        release_id = basic_info.get('id', None)
        title = basic_info.get('title', None)
        artist = basic_info.get('artists', [{}])[0].get('name', None)
        year = basic_info.get('year', None)
        label_info = basic_info.get('labels', [{}])[0]
        label = label_info.get('name', None)
        catno = label_info.get('catno', None)
        added = release.get('date_added', None)

        notes = release.get('notes', [])

        media_condition = notes[0]['value'] if notes and len(notes) > 0 else None
        sleeve_condition = notes[1]['value'] if notes and len(notes) > 1 else None
        additional_notes = notes[2]['value'] if notes and len(notes) > 2 else None

        resource_url = basic_info.get('resource_url', None)

        # Split additional_notes into three columns
        if additional_notes:
            # Try to split based on the provided example string
            split_data = additional_notes.split(', ')

            if len(split_data) == 3:
                price_paid, vendor, date_of_purchase = split_data
            else:
                # Handle cases where the convention is not followed
                price_paid, vendor, date_of_purchase = None, None, None
        else:
            # Handle cases where additional_notes is not present
            price_paid, vendor, date_of_purchase = None, None, None

        cursor.execute(f"""
            INSERT INTO {schema_name}.{table_name} 
            (release_id, artist, title, year, label, catno, added, media_condition, sleeve_condition, additional_notes, 
            price_paid, vendor, date_of_purchase, resource_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (release_id, artist, title, year, label, catno, added, media_condition, sleeve_condition, additional_notes,
              price_paid, vendor, date_of_purchase, resource_url))

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

conn.commit()

# Function to fetch price suggestions and insert them into the database
def get_and_insert_price_suggestions(release_id, media_condition):
    # Get price suggestions from Discogs API
    endpoint_price = f'https://api.discogs.com/marketplace/price_suggestions/{release_id}'

    price_suggestions_data = requests.get(endpoint_price, headers=headers)

    # Convert the string to a dictionary
    price_suggestions_dict = json.loads(price_suggestions_data)

    # Iterate through the conditions and values
    for condition, condition_data in price_suggestions_dict.items():
        value = condition_data.get("value")

        # Insert price suggestions into the database
        cursor.execute(f"""
            UPDATE {schema_name}.{table_name}
            SET price_suggestion = %s
            WHERE release_id = %s AND media_condition = %s
        """, (value, release_id, media_condition))
    else:
        print(f"Failed to retrieve price suggestions for release_id={release_id} and media_condition={media_condition}")



# Iterate through the records in the database
cursor.execute(f"SELECT release_id, media_condition FROM {schema_name}.{table_name}")
for row in cursor.fetchall():
    release_id, media_condition = row
    get_and_insert_price_suggestions(release_id, media_condition)

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

# else:
#   print(f"Error: {response.status_code}, {response.text}")
