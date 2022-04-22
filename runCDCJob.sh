#!/bin/bash
#echo "reset job bookmark"

#todo error handling - An error occurred (EntityNotFoundException) when calling the ResetJobBookmark operation: Continuation for job MeterMeasurementsHudiCompactionJob not found
#aws glue reset-job-bookmark --job-name MeterMeasurementsHudiCompactionJob

echo "running job"
aws glue start-job-run --job-name MeterMeasurementsHudiCompactionJob