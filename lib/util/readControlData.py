import boto3
import json
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Key

dynamodb_r = boto3.resource('dynamodb') 

##todo - add error handling
compactJobParamItems = dynamodb_r.Table("GlueControlTable").query(
    KeyConditionExpression=Key('glue_job_name').eq("MeterMeasurementsHudiCompactionJob")
)

print(compactJobParamItems['Items'])

# for compactJobParams in compactJobParamItems['Items']:
#     print("printing job params from dynamo db")
#     print(compactJobParams)
