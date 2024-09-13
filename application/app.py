import boto3
import os
import json
import requests
import pandas as pd
from io import StringIO

def flatten_data(data):
    flattened = []
    for item in data:
        flat_item = {
            "id": item["id"],
            "type": item["type"],
            "name": item["attributes"]["name"],
            "description": item["attributes"]["description"],
            "life_max": item["attributes"]["life"]["max"],
            "life_min": item["attributes"]["life"]["min"],
            "male_weight_max": item["attributes"]["male_weight"]["max"],
            "male_weight_min": item["attributes"]["male_weight"]["min"],
            "female_weight_max": item["attributes"]["female_weight"]["max"],
            "female_weight_min": item["attributes"]["female_weight"]["min"],
            "hypoallergenic": item["attributes"]["hypoallergenic"],
            "group_id": item["relationships"]["group"]["data"]["id"],
            "group_type": item["relationships"]["group"]["data"]["type"]
        }
        flattened.append(flat_item)
    return flattened


def handler(event, context):
    s3_bucket = os.environ.get('s3_bucket')
    api_url = "https://dogapi.dog/api/v2/breeds"  # Replace with your API URL
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()  # Parse JSON data from the API response

        # Process the data
        data_list = flatten_data(data["data"])

        # Convert the list of dict to df
        df = pd.DataFrame(data_list)

        # Create DataFrame
        df = pd.DataFrame(data_list)

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # S3 configuration
        s3_client = boto3.client('s3')
        bucket_name = s3_bucket
        s3_file_key = 'files/dog_breed.csv'

        # Upload CSV to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=csv_buffer.getvalue())

        print(f"File uploaded to S3 bucket '{bucket_name}' at '{s3_file_key}'")


    except requests.exceptions.HTTPError as http_err:
        return {
            'statusCode': response.status_code,
            'body': json.dumps({"error": str(http_err)})
        }

    except Exception as err:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(err)})
        }






