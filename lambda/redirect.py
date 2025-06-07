import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from datetime import datetime
import re
import json
import os

dynamodb = boto3.resource('dynamodb')

def read_hash_url(hash_value, table_name=os.environ.get("DDB_TABLE")) -> str:
    table = dynamodb.Table(table_name)
    try:
        # Put the item into the table
        t1 = datetime.now()
        response = table.get_item(
            Key={
                'hash': hash_value,
            }
        )
        item = response.get('Item')
        t2 = datetime.now()
        print(f"Data successfully retrieved from DynamoDB.  Took {t2-t1} sec")
        return item["url"]
    except ClientError as e:
        print(f"Failed to read from DynamoDB: {e.response['Error']['Message']}")
        return ""

def get_hash_from_url(url, validate=True):
    path = urlparse(url).path
    pattern = r"^/r/([a-fA-F0-9]+)/*"
    match = re.match(pattern, path)
    if match:
        hex_str = match.group(1)
        return path.split("/")[2]
    else:
        return None

def lambda_handler(event, context):
    url = event.get('rawPath') or event.get('path', '')
    hash_short = get_hash_from_url(url)
    print(hash_short)
    redirect_url = read_hash_url(hash_short)
    print(f"Redirect URL: {redirect_url}")
    
    return {
        "statusCode": 302,
        "headers": {
            "Location": redirect_url,
        }
    }

