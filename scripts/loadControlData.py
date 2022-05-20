import boto3
import json
from boto3.dynamodb.conditions import Key
import sys

dynamodb_r = boto3.resource('dynamodb') 
tableName = sys.argv[1]
table = dynamodb_r.Table(tableName)

f = open('scripts/config/control_file.json')
request_items = json.loads(f.read())
for item in request_items:
    print("....=", item['glue_job_name'])
    table.put_item(Item=item)
