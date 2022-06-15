#!/bin/bash
cd ~/environment/glue-hudi/scripts

echo "Copy incremental data file to raw bucket"
#get rawBucket name
rawBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkblograwbucket")) | .OutputValue '`
aws s3 cp data/measurement_data_cdc.parquet s3://$rawBucket/msrmt_db/msrmt_schema/msrmt_table/CDC.parquet

echo "Running MeterMeasurementsHudiProcessingJob Glue Job"
aws glue start-job-run --job-name MeterMeasurementsHudiProcessingJob
jobRunStatus=$?

if [ $jobRunStatus -eq 0 ]
then
    echo "MeterMeasurementsHudiProcessingJob Glue Job invoked successfully"
else
    echo "MeterMeasurementsHudiProcessingJob Glue Job invoke failed"
fi
