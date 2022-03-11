import boto3
import json
from boto3.dynamodb.conditions import Key

dynamodb_r = boto3.resource('dynamodb') 
#todo ...get table name
table = dynamodb_r.Table('CdkGlueTestStack-gluetable2B8EEA762-1S25F7QEEIW6M')

f = open('sample_control_data.json')
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
