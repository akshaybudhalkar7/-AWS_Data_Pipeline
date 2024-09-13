import boto3
import os
import json
import requests
import pandas as pd
from io import StringIO
import time

glue_client = boto3.client('glue', region_name='us-east-1')
s3_client = boto3.client('s3')

def check_crawler_status(crawler_name):
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        return response['Crawler']['State']
    except Exception as e:
        print(f"Error checking Crawler status: {str(e)}")
        raise e

def trigger_glue_job(job_name):
    try:
        response = glue_client.start_job_run(JobName=str(job_name))
        print(f"Started Glue Job: {response['JobRunId']}")
        return response['JobRunId']
    except Exception as e:
        print(f"Error starting Glue Job: {str(e)}")
        raise e


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
    crawler = os.environ.get('crawler')
    glue_job_name = os.environ.get('glue_job')

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

        bucket_name = s3_bucket
        s3_file_key = 'dog_data/dog_breed.csv'

        # Upload CSV to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=csv_buffer.getvalue())

        print(f"File uploaded to S3 bucket '{bucket_name}' at '{s3_file_key}'")

        print(f"Start Crawler{crawler}")

        response = glue_client.start_crawler(Name=crawler)

        print('Started Crawler')

        # Check Crawler status every 30 seconds until it is complete
        while True:
            crawler_state = check_crawler_status(crawler)
            print(f"Crawler {crawler} is in {crawler_state} state.")

            if crawler_state == 'READY':
                print("glue_job_name",glue_job_name)
                # If the crawler is done, trigger the Glue Job
                job_run_id = trigger_glue_job(glue_job_name)
                print(f"Glue Job {glue_job_name} triggered with run ID {job_run_id}")
                break

            # Wait for 30 seconds before checking again
            time.sleep(30)

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






