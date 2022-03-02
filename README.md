# CDK TypeScript project for deploying Glue ETL jobs.


## Instructions

git clone git@github.com:nikhilpenmetsa/glue-hudi.git
cd glue-hudi
npm install
cdk bookstrap



Update CDK to latest 2.13.0 (build b0b744d) - as of 02/25.
Add CDK IAM role as "Database creator" in lakeformation console


## Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template
