# CDK TypeScript project for deploying Glue ETL jobs.

## Instructions
 * `git clone git@github.com:nikhilpenmetsa/glue-hudi.git`  clone repository
 * Install CDK - https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
 * `cd glue-hudi`   cd to proj directory
 * `npm install`    install package dependencies
 * `./deployConfigStack.sh`  setup script to bootstrap cdk, install jq, create buckets, copy jar files
 * `./deployGlueStack.sh` deploy stack to create buckets and dynamodb table
 * `./runJob.sh` runs job
 * `./reset.sh` temporary cleanup to delete glue DB and delete data in processed buckets before running job again


