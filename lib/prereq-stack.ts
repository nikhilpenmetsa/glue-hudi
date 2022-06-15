import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Role, ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import { Bucket, BucketAccessControl, BucketEncryption, BlockPublicAccess  } from 'aws-cdk-lib/aws-s3';
import {Source ,BucketDeployment} from 'aws-cdk-lib/aws-s3-deployment'
import { Asset } from "aws-cdk-lib/aws-s3-assets";

import { Table, AttributeType, BillingMode } from 'aws-cdk-lib/aws-dynamodb'


export class PreReqStack extends Stack {
  public glueRoleGrantReadWrite: Role;
  public rawBucket: Bucket;
  public processedBucket: Bucket;
  public libraryBucket: Bucket;

  public controlTable: Table;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);


    //Create role
    const glueRoleGrantReadWrite = new Role(this, 'access-glue-role', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com')
    });
    glueRoleGrantReadWrite.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    this.glueRoleGrantReadWrite = glueRoleGrantReadWrite;    
    
    //Create bucket to hold raw data. The data in this bucket will be the input dataset for the glue job.
    const rawBucket = new Bucket(this, 'hudi-framework-blog-raw-bucket', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.rawBucket = rawBucket;
 
    //Create bucket to hold processed data. This is the output from the glue job.
    const processedBucket = new Bucket(this, 'hudi-framework-blog-processed-bucket', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.processedBucket = processedBucket;
 
    //Create bucket to hold libraries, scripts used by the glue job.
    const libraryBucket = new Bucket(this, 'hudi-framework-blog-lib-bucket', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.libraryBucket = libraryBucket;

    //populate library S3 bucket with glue job script,.
    new BucketDeployment(this, 'script',{
      sources: [
        Source.asset("lib/assets/scripts")],
      destinationBucket: libraryBucket,
      destinationKeyPrefix: 'scripts'
    })
    

  //stage raw data for initial load
    new BucketDeployment(this, 'staging_data',{
      sources: [
        Source.asset("lib/assets/data")],
      destinationBucket: rawBucket,
      destinationKeyPrefix: 'msrmt_db/msrmt_schema/msrmt_table'
    })
 
 
    //create DynamoDB table to hold job control details.
    const controlTable = new Table(this, 'jobControlTable', {
      partitionKey: {name: 'glue_job_name', type: AttributeType.STRING},
      sortKey: {name: 'table_name', type: AttributeType.STRING},
      billingMode: BillingMode.PAY_PER_REQUEST, 
    })
    this.controlTable = controlTable;

  }
}

export interface PreReqStackProps extends StackProps {
  glueRoleGrantReadWrite: Role;
  rawBucket: Bucket;
  processedBucket: Bucket;
  libraryBucket: Bucket;
  controlTable: Table;
}
