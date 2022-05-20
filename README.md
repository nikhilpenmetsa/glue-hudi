# CDK TypeScript project for deploying Glue ETL jobs.

## Instructions
 * `git clone git@github.com:nikhilpenmetsa/glue-hudi.git`  clone repository
 * Install CDK - https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
 * `cd glue-hudi/scripts`   cd to scripts directory
 * `./deployConfigStack.sh` setup script to bootstrap cdk, install jq, create buckets, copy jar files, create dynamodb table and load data into table
 * `./deployGlueStack.sh`   script to deploy glue job
 * `./runJobForInitialLoad.sh`  script to run job for initial load
 * `./runJobForIncrementalLoad.sh`  script to copy incremental data file to raw bucket and to run job for processing incremental data
 * 
 
## Cleanup
 * `./resetForInitialLoad.sh`   script to delete artifacts in Glue catalog, empty processed bucket.
 * `./cleanup.sh`   script to cleanup all artifacts.



