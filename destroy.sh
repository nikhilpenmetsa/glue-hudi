#!/bin/bash
cd ~/environment/glue-hudi

glueControlTable=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefjobControlTable")) | .OutputValue '`
echo $?

echo $glueControlTable

aws dynamodb describe-table --table-name $glueControlTable
echo $?

echo "Deleting dynamodb table: " $glueControlTable
aws dynamodb delete-table --table-name $glueControlTable

echo "Running cdk destroy --all"
cdk destroy --all --require-approval never