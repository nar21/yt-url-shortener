import hashlib
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import json
import os

dynamodb = boto3.resource('dynamodb')

def store_hash_url(hash_value, url, table_name=os.environ.get("DDB_TABLE")):
    # Get the table reference
    table = dynamodb.Table(table_name)

    try:
        # Put the item into the table
        t1 = datetime.now()
        response = table.put_item(
            Item={
                'hash': hash_value,
                'url': url
            }
        )
        t2 = datetime.now()
        print(f"Data successfully written to DynamoDB. Took {t2-t1} sec")
    except ClientError as e:
        print(f"Failed to write to DynamoDB: {e.response['Error']['Message']}")

def get_hash(username, redirect_url, timestamp):
    hash_input =  (username + redirect_url + timestamp).encode("ascii")
    return hashlib.sha256(hash_input).hexdigest()

def lambda_handler(event, context):
    request_body = event
    
    BASE_URL = os.environ.get("BASE_URL")
    redirect_url = request_body["redirect_url"]
    print(BASE_URL, redirect_url)
    
    t0 = datetime.now()
    hash = get_hash("naresh", redirect_url, str(datetime.now()))
    t1 = datetime.now()
    
    hash_short = hash[:16]
    shortened_url = f"{BASE_URL}/r/{hash_short}"
    store_hash_url(hash_short, redirect_url)
    t2 = datetime.now()
    print(f"Short URL: shortened_url")
    
    print("t2-t1: " + str(t2-t1))
    print("t1-t0: " + str(t1-t0))
    return {
        'statusCode': 200,
        'body': {"shortened_url": shortened_url}
        
    }
