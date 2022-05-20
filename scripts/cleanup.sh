#!/bin/bash
cd ~/environment/glue-hudi/
echo "Cleaning up deployment"

echo "Deleting dynamodb table: " $glueControlTable
glueControlTable=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefjobControlTable")) | .OutputValue '`
aws dynamodb delete-table --table-name $glueControlTable
echo "Deleted dynamodb table: " $glueControlTable

echo "Deleting glue db dl_msrmt_db_msrmt_schema"
aws glue delete-database --name dl_msrmt_db_msrmt_schema
echo "Deleted glue db dl_msrmt_db_msrmt_schema"


echo "Deleting CDK stacks"
cdk destroy --all --force
cdkDestroyStatus=$?

if [ $cdkDestroyStatus -eq 0 ]
then
    echo "Cleanup completed successfully"
else
    echo "Cleanup failed"
fi
