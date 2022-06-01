# Serverless CDC processing framework using AWS Glue and Apache Hudi.
Project demonstrating a CDC processing framework. Framework uses Apache Hudi with AWS Glue to apply record-level inserts, updates, and deletes from their source systems into their S3 datalake without always-on RDBMS targets.

## Instructions
 * `git clone git@github.com:nikhilpenmetsa/glue-hudi.git`  clone repository
 * Install CDK - https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
 * `cd glue-hudi/scripts`   cd to scripts directory
 * `./deployConfigStack.sh` setup script to bootstrap cdk, install jq, create buckets, copy jar files, create dynamodb table and load data into table
 * `./deployGlueStack.sh`   script to deploy glue job
 * `./runJobForInitialLoad.sh`  script to run job for initial load
 * `./runJobForIncrementalLoad.sh`  script to copy incremental data file to raw bucket and to run job for processing incremental data
 * `./resetForInitialLoad.sh` script to re-run the job for initial load. Script delete artifacts in Glue catalog, empty processed bucket.
 
## Cleanup
 * `./cleanup.sh`   script to cleanup all artifacts.



