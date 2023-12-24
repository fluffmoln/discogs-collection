import requests
import urllib3

# Replace 'YOUR_API_KEY' and 'YOUR_USER_AGENT' with your Discogs API key and a user-agent string
API_KEY = 'tvagGfDrinEkSmQHDbrDoZbvKKhVDZUVCPvwdMzM'
USER_AGENT = 'fluffmoln-discogs-collection'

# Set up the API endpoint and headers
#endpoint = 'https://api.discogs.com/users/{username}/collection/folders/0/releases'
endpoint_folders = 'https://api.discogs.com/users/{username}/collection/folders'
headers = {
    'User-Agent': USER_AGENT,
    'Authorization': f'Discogs token={API_KEY}'
}

# Replace 'YOUR_USERNAME' with your Discogs username
username = 'fluffmoln'

# Make the API request
response = requests.get(endpoint_folders.format(username=username), headers=headers)

print(f'{response}')

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Extract and print information about each release in the collection
#    for release in data['releases']:
#        print(f"Title: {release['basic_information']['title']}")
#        print(f"Artist: {release['basic_information']['artists'][0]['name']}")
#        print(f"Year: {release['basic_information']['year']}")
#        print(f"Label: {release['basic_information']['labels'][0]['name']}")
#        print("-" * 50)
    for folder in data['folders']:
        folder_id = folder['id']
        folder_name = folder['name']
        print(f"Folder ID: {folder_id}, Folder Name: {folder_name}")
else:
    print(f"Error: {response.status_code}, {response.text}")
