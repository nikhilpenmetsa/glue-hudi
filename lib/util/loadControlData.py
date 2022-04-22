import boto3
import json
from boto3.dynamodb.conditions import Key
import sys

dynamodb_r = boto3.resource('dynamodb') 
#todo ...get table name
tableName = sys.argv[1]
#print(tableName)
#print("all arg", str(sys.argv))
#table = dynamodb_r.Table('GlueControlTable')
table = dynamodb_r.Table(tableName)
#print("dynamodb table", table)

f = open('lib/assets/config/control_file.json')
request_items = json.loads(f.read())
# response = dynamodb_c.batch_write_item(RequestItems=request_items)
# print(len(request_items))
for item in request_items:
    print("....=", item['glue_job_name'])
    table.put_item(Item=item)




# compactJobParamItems = dynamodb_r.Table("GlueCompactionTable2").query(
#     KeyConditionExpression=Key('glue_job_name').eq("glueJob-datalake-cisdev-etl01")
# )

# #print(compactJobParamItems)

# for compactJobParams in compactJobParamItems['Items']:
#     print(compactJobParams)
