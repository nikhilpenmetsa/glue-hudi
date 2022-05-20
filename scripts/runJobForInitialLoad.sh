#!/bin/bash
echo "Run MeterMeasurementsHudiCompactionJob Glue Job"
aws glue start-job-run --job-name MeterMeasurementsHudiCompactionJob
jobRunStatus=$?

if [ $jobRunStatus -eq 0 ]
then
    echo "MeterMeasurementsHudiCompactionJob Glue Job invoked successfully"

else
    echo "MeterMeasurementsHudiCompactionJob Glue Job invoke failed"
fi