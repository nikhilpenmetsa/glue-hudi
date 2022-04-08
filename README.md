# CDK TypeScript project for deploying Glue ETL jobs.

## Instructions
 * `git clone git@github.com:nikhilpenmetsa/glue-hudi.git`  clone repository
 * Install CDK - https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
 * `cd glue-hudi`   cd to proj directory
 * `./deployConfigStack.sh`  setup script to bootstrap cdk, install jq, create buckets, copy jar files, create dynamodb table and load data into table
 * `./deployGlueStack.sh` deploy glue job
 * `./runJob.sh` runs job
 * `./reset.sh` temporary cleanup to delete glue DB and delete data in processed buckets before running job again
 * 
 
## Cleanup
 * `./reset.sh` deletes glue catalog, empties processed bucket
 * `./destroy.sh` deletes DynamoDB table and 2 stacks.



