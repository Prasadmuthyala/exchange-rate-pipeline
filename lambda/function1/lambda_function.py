import requests
import json
import boto3
from datetime import datetime

# Create an S3 client using boto3
s3_client = boto3.client('s3')

def functioncall():
    url = "https://v6.exchangerate-api.com/v6/fcb5065dac591099dbaaa682/latest/USD"
    print('Requesting exchange rate data...')
    
    try:
        response = requests.get(url)  # Send the GET request to the API
        res = response.json()  # Convert the response to JSON
        
        if res['result'] == 'success':  # Check if the response was successful
            # Extract and format the exchange rates
            exchange_rates = res['conversion_rates']
            # Define the S3 bucket name and the key (file name) for the JSON file
            bucket_name = 'exchangerateapi'  # Replace with your actual bucket name
            file_key = f"raw_data_source/results/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_result.json"
            result_json=json.dumps({
                'statusCode': 200,
                'body': json.dumps({
                    'conversion_rates': exchange_rates,
                    'time_last_update_utc': res['time_last_update_utc'],
                    'time_next_update_utc': res['time_next_update_utc'],
                    "timestamp": str(datetime.now())
                })
            })
            # Upload the JSON file to S3
            s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=result_json,
            ContentType='application/json'
            )

            return result_json
        else:
            # If the result is not successful, return an error message
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Error fetching data from API'})
            }
    
    except requests.exceptions.RequestException as e:
        # Handle any request-related errors
        return {
            'statusCode': 500,
            'body': json.dumps({'message': str(e)})
        }

# Lambda handler function
def lambda_handler(event, context):
    return functioncall()
