echo "reset"
echo "deleting glue db dl_msrmt_db_msrmt_schema"
aws glue delete-database --name dl_msrmt_db_msrmt_schema

echo "empty processed s3 bucket"
processedBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefnpprocessedbucket")) | .OutputValue '`

aws s3 rm s3://$processedBucket --recursive
echo "reset done"