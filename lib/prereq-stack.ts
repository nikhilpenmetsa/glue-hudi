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



    const glueRoleGrantReadWrite = new Role(this, 'access-glue-avista', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
      roleName: "GlueHudiRole"
    });
    glueRoleGrantReadWrite.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    this.glueRoleGrantReadWrite = glueRoleGrantReadWrite;    
    
    const rawBucket = new Bucket(this, 'np-raw-bucket123', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.rawBucket = rawBucket;
 
    const processedBucket = new Bucket(this, 'np-processed-bucket123', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.processedBucket = processedBucket;
 
    const libraryBucket = new Bucket(this, 'np-libs-bucket123', {
      accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    });
    this.libraryBucket = libraryBucket;

    //populate library S3 bucket with jars, scripts and control file.
    //https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_s3_deployment-readme.html
    new BucketDeployment(this, 'deployJars',{
      sources: [
        // Source.asset("lib/assets/jars/hudi-spark-bundle_2.11-0.10.1.jar"), 
        // Source.asset("lib/assets/jars/spark-avro_2.11-2.4.4.jar"), 
        // Source.asset("lib/assets/jars/log4j-web-2.16.0.jar")
        ],
      destinationBucket: libraryBucket,
      destinationKeyPrefix: 'jars'
    })
    
    new BucketDeployment(this, 'script',{
      sources: [
        Source.asset("lib/assets/scripts")],
      destinationBucket: libraryBucket,
      destinationKeyPrefix: 'scripts'
    })
    
    new BucketDeployment(this, 'control_file',{
      sources: [
        Source.asset("lib/assets/config/")],
      destinationBucket: libraryBucket,
      destinationKeyPrefix: 'config'
    })
 
  //stage raw data
    new BucketDeployment(this, 'staging_data',{
      sources: [
        Source.asset("lib/assets/data")],
      destinationBucket: rawBucket,
      destinationKeyPrefix: 'msrmt_db/msrmt_schema/msrmt_table'
    })
 
 

    const controlTable = new Table(this, 'gluetable2', {
      partitionKey: {name: 'glue_job_name', type: AttributeType.STRING},
      sortKey: {name: 'tablename_and_pk', type: AttributeType.STRING},
      tableName : "GlueControlTable",
      billingMode: BillingMode.PAY_PER_REQUEST, 
    })
    this.controlTable = controlTable;

    // rawBucket.grantReadWrite(glueHudiRole);
    // processedBucket.grantReadWrite(glueHudiRole);
    // controlTable.grantReadData(glueHudiRole);

  }
}

export interface PreReqStackProps extends StackProps {
  glueRoleGrantReadWrite: Role;
  rawBucket: Bucket;
  processedBucket: Bucket;
  libraryBucket: Bucket;
  controlTable: Table;
}


