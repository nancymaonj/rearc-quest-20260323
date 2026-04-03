# 1. Create a script that will fetch data from 
# this API (https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population) . 
# You can read the documentation here (https://datausa.io/about/api/)
# 2. Save the result of this API call as a JSON file in S3 (s3://rearc-quest-20260323-876784288665-us-east-1-an/quest-part-2/).

# to enable window environment is configured with AWS credentials
# install aws cli and its credentials, or set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

# output: https://rearc-quest-20260323-876784288665-us-east-1-an.s3.us-east-1.amazonaws.com/quest-part-2/population_data.json

# Create a python script that will fetch data from 
# this API 
# (https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population) . 
#  https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population
# Save the result of this API call as a JSON file in directory ("bls_downloads-20260403" ) under the current working directory.
# the script should be able to run in windows environment and avoid access forbidden issue by setting user agent in the request header.

import urllib.request
import urllib.parse
import json
import os

def fetch_datausa_records():
    # 1. Configuration
    # Updated the path extension from .json to .jsonrecords as per the error message
    base_url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    
    params = {
        "cube": "acs_yg_total_population_1",
        "drilldowns": "Year,Nation",
        "locale": "en",
        "measures": "Population"
    }
    
    target_dir = "bls_downloads-20260403"
    file_name = "population_records.json"
    
    # Headers to avoid 403 Forbidden
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }

    try:
        # 2. Build the encoded URL
        query_string = urllib.parse.urlencode(params)
        full_url = f"{base_url}?{query_string}"
        
        # 3. Handle Windows Directory Path
        full_dir_path = os.path.normpath(os.path.join(os.getcwd(), target_dir))
        if not os.path.exists(full_dir_path):
            os.makedirs(full_dir_path)

        print(f"Requesting: {full_url}")

        # 4. Fetch and Save
        req = urllib.request.Request(full_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        target_file_path = os.path.join(full_dir_path, file_name)
        with open(target_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
            
        print(f"Successfully saved data to: {target_file_path}")

    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason}")
        print(e.read().decode())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_datausa_records()