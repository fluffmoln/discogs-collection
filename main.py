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


# Define function to insert data into PostgreSQL
def insert_data(release_data):
    for release in release_data['releases']:
        title = release['basic_information']['title']
        artist = release['basic_information']['artists'][0]['name']
        year = release['basic_information']['year']

        cursor.execute(f"""
            INSERT INTO {schema_name}.{table_name} (title, artist, year)
            VALUES (%s, %s, %s)
        """, (title, artist, year))


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

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

# else:
#   print(f"Error: {response.status_code}, {response.text}")
