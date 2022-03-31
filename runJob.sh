echo "reset job bookmark"

aws glue reset-job-bookmark --job-name MeterMeasurementsHudiCompactionJob

echo "running job"
aws glue start-job-run --job-name MeterMeasurementsHudiCompactionJob