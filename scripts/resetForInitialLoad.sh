#!/bin/bash
echo "Resetting job for initial load"
echo "Deleting glue db dl_msrmt_db_msrmt_schema"
aws glue delete-database --name dl_msrmt_db_msrmt_schema

echo "Empty processed s3 bucket"
processedBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkblogprocessedbucket")) | .OutputValue '`

aws s3 rm s3://$processedBucket --recursive
echo "Reset completed"