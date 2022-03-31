#!/bin/bash
cd ~/environment/glue-hudi
echo "bootstrap cdk"
cdk bootstrap

echo "Installing jq..."
sudo yum install -y jq > /dev/null 2>&1

echo "Deploying staging stack - buckets, scripts.."
cdk deploy PreReqStack
jarBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefnplibsbucket")) | .OutputValue '`


echo "uploading jars to " $jarBucket " bucket"
aws s3 cp lib/assets/jars/log4j-web-2.16.0.jar s3://$jarBucket/jars/
aws s3 cp lib/assets/jars/spark-avro_2.11-2.4.4.jar s3://$jarBucket/jars/
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark-bundle_2.11/0.10.1/hudi-spark-bundle_2.11-0.10.1.jar
aws s3 cp hudi-spark-bundle_2.11-0.10.1.jar s3://$jarBucket/jars/
echo "completed uploading"  

rm hudi-spark-bundle_2.11-0.10.1.jar