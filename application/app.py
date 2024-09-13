import boto3
import os
import json
import requests


def lambda_handler(event, context):
    api_url = "https://dogapi.dog/api/v2/breeds"  # Replace with your API URL
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()  # Parse JSON data from the API response

        # Process the data or return it
        return {
            'statusCode': 200,
            'body': json.dumps(data)
        }

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




