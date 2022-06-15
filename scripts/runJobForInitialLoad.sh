#!/bin/bash
echo "Run MeterMeasurementsHudiProcessingJob Glue Job"
aws glue start-job-run --job-name MeterMeasurementsHudiProcessingJob
jobRunStatus=$?

if [ $jobRunStatus -eq 0 ]
then
    echo "MeterMeasurementsHudiProcessingJob Glue Job invoked successfully"

else
    echo "MeterMeasurementsHudiProcessingJob Glue Job invoke failed"
fi