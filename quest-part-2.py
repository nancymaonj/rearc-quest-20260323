# 1. Create a script that will fetch data from 
# this API (https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population) . 
# You can read the documentation here (https://datausa.io/about/api/)
# 2. Save the result of this API call as a JSON file in S3 (s3://rearc-quest-20260323-876784288665-us-east-1-an/quest-part-2/).

# to enable window environment is configured with AWS credentials
# install aws cli and its credentials, or set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

import requests
import boto3
import json
from botocore.exceptions import ClientError

def fetch_and_upload_to_s3():
    # 1. Configuration
    api_url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    bucket_name = "rearc-quest-20260323-876784288665-us-east-1-an"
    s3_key = "quest-part-2/population_data.json"

    try:
        # 2. Fetch data from the API
        print(f"Fetching data from: {api_url}")
        response = requests.get(api_url)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        data = response.json()

        # 3. Initialize S3 client
        # Note: Ensure your environment is configured with AWS credentials
        s3_client = boto3.client('s3')

        # 4. Upload the JSON data to S3
        print(f"Uploading data to s3://{bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )

        print("Successfully uploaded API results to S3.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_and_upload_to_s3()