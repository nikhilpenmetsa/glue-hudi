import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Role, ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
//import * as s3 from '@aws-cdk/aws-s3';
import { Bucket, BucketAccessControl, BucketEncryption, BlockPublicAccess  } from 'aws-cdk-lib/aws-s3';
import * as glue from "@aws-cdk/aws-glue";
import { Asset } from "@aws-cdk/aws-s3-assets";
import * as path from "path";


export class CdkGlueTestStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const role = new Role(this, 'access-glue-fifa', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com')
    });
    
    //const gluePolicy = new ManagedPolicy(fromAwsManagedPolicyName("service-role/AWSGlueServiceRole");
    role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    
    const rawBucket = new Bucket(this, 'np-raw-bucket123', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
 
    const processedBucket = new Bucket(this, 'np-processed-bucket123', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
 
    rawBucket.grantReadWrite(role);
    processedBucket.grantReadWrite(role);
    
    const f_pyAssetETL = new Asset(this, "hello-etl", {
      path: path.join(__dirname, "assets/hello-etl.py"),
    })
 
  }
}

