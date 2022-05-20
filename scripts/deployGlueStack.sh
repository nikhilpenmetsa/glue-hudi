#!/bin/bash
cd ~/environment/glue-hudi/scripts

echo "Deploying Glue job stack..."
cdk deploy CdkGlueTestStack
cdkDeployStatus=$?

if [ $cdkDeployStatus -eq 0 ]
then
    echo "Glue job stack deployment completed successfully"
else
    echo "Glue job stack deployment failed"
fi

