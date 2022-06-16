#!/bin/bash
cd ~/environment/glue-hudi

echo "install npm packages"
npm install

echo "bootstrap cdk"
cdk bootstrap

echo "Installing jq..."
sudo yum install -y jq > /dev/null 2>&1

echo "Deploying Configuration stack..."
cdk deploy PreReqStack --require-approval never

cdkDeployStatus=$?

if [ $cdkDeployStatus -eq 0 ]
then
    glueRole=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputFnGetAttaccessgluerole")) | .OutputValue '`
    rawBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkblograwbucket")) | .OutputValue '`
    processedBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkblogprocessedbucket")) | .OutputValue '`
    libBucket=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefhudiframeworkbloglibbucket")) | .OutputValue '`
    glueControlTable=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefjobControlTable")) | .OutputValue '`

    echo "Created role: " $glueRole
    echo "Created S3 bucket to hold raw data: " $rawBucket
    echo "Created S3 bucket to hold processed data: " $processedBucket
    echo "Created S3 bucket to hold library jar files: " $libBucket
    echo "Created DynamoDB table to hold job control configurations: " $glueControlTable
    
    echo "Uploading jars to " $libBucket " bucket"
    wget https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-web/2.16.0/log4j-web-2.16.0.jar
    aws s3 cp log4j-web-2.16.0.jar s3://$libBucket/jars/
    
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.4/spark-avro_2.11-2.4.4.jar
    aws s3 cp spark-avro_2.11-2.4.4.jar s3://$libBucket/jars/
    
    wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark-bundle_2.11/0.10.1/hudi-spark-bundle_2.11-0.10.1.jar
    aws s3 cp hudi-spark-bundle_2.11-0.10.1.jar s3://$libBucket/jars/
    echo "Completed uploading jars to " $libBucket " bucket"

    rm log4j-web-2.16.0.jar
    rm spark-avro_2.11-2.4.4.jar
    rm hudi-spark-bundle_2.11-0.10.1.jar
    
    echo "Populating job control Configs into DynamoDB table "
    glueControlTable=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefjobControlTable")) | .OutputValue '`

    pip3 install -r requirements.txt
    python3 scripts/loadControlData.py $glueControlTable
    echo "Populated job control Configs into DynamoDB table "
    
    echo "Configuration stack deployment completed successfully"
else
    echo "Configuration stack deployment failed"
fi

