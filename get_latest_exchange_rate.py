import requests
import json


def fetch_and_store_data(url, filename):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()

        with open(filename, 'w') as file:
            json.dump(data, file)

        print(f"Data successfully written to {filename}")
    except Exception as e:
        print(f"Error occurred: {e}")


# Replace with your actual URL
url = 'https://open.er-api.com/v6/latest/SGD'
filename = 'latest.json'

fetch_and_store_data(url, filename)
