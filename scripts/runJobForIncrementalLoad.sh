#!/bin/bash
cd ~/environment/glue-hudi/scripts

echo "Copy incremental data file to raw bucket"
#get rawBucket name
rawBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkblograwbucket")) | .OutputValue '`
aws s3 cp data/cdc_measurement_data_0002.parquet s3://$rawBucket/msrmt_db/msrmt_schema/msrmt_table

echo "Running MeterMeasurementsHudiCompactionJob Glue Job"
aws glue start-job-run --job-name MeterMeasurementsHudiCompactionJob
jobRunStatus=$?

if [ $jobRunStatus -eq 0 ]
then
    echo "MeterMeasurementsHudiCompactionJob Glue Job invoked successfully"
else
    echo "MeterMeasurementsHudiCompactionJob Glue Job invoke failed"
fi
