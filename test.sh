#!/bin/bash
cd ~/environment/glue-hudi

glueControlTable=`aws cloudformation describe-stacks --stack-name prereqStack --query "Stacks[0].Outputs" --output json | jq -rc '.[] | select(.OutputKey | startswith("ExportsOutputRefgluetable")) | .OutputValue '`

echo $glueControlTable
python lib/util/loadControlData.py $glueControlTable