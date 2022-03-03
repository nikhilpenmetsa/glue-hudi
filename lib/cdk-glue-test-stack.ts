import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Role, ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
//import * as s3 from '@aws-cdk/aws-s3';
import { Bucket, BucketAccessControl, BucketEncryption, BlockPublicAccess  } from 'aws-cdk-lib/aws-s3';
//import * as glue from "@aws-cdk/aws-glue";
//import { Database } from "aws-cdk-lib/aws-glue";
//import { CfnDatabase,CfnDatabaseProps } from 'aws-cdk-lib/aws-glue';

import { Database, Job, JobProps, JobExecutable, GlueVersion, PythonVersion,Code } from '@aws-cdk/aws-glue-alpha';

import { Asset } from "aws-cdk-lib/aws-s3-assets";
import * as path from "path";


export class CdkGlueTestStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const role = new Role(this, 'access-glue-avista', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com')
    });
    
    //const gluePolicy = new ManagedPolicy(fromAwsManagedPolicyName("service-role/AWSGlueServiceRole");
    role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    
    // const rawBucket = new Bucket(this, 'np-raw-bucket123', {
    //   accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
    //   encryption: BucketEncryption.S3_MANAGED,
    //   blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    // });
 
    // const processedBucket = new Bucket(this, 'np-processed-bucket123', {
    //   accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
    //   encryption: BucketEncryption.S3_MANAGED,
    //   blockPublicAccess: BlockPublicAccess.BLOCK_ALL
    // });
 
    const importBucket = Bucket.fromBucketName(this, "np-glue-hudi-raw2-id", "np-glue-hudi-raw2")
    const exportBucket = Bucket.fromBucketName(this, "np-glue-hudi-processed2-id", "np-glue-hudi-processed2")

    // rawBucket.grantReadWrite(role);
    // processedBucket.grantReadWrite(role);
    importBucket.grantReadWrite(role);
    exportBucket.grantReadWrite(role);
    
    // console.log("------rawBucket--------",rawBucket)
    console.log("---------------importBucketName--------------------",importBucket)
    console.log("---------------done---importBucketName--------------------")



    const job = new Job(this, "glue-job-asset", {
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V3_0,
        pythonVersion: PythonVersion.THREE,
        script: Code.fromAsset(path.join(__dirname, "assets/hello-etl.py"))
      }),
      role: role,
      jobName: 'a-glue-cdk-job',
      maxRetries: 0,
      maxConcurrentRuns: 1,
      defaultArguments: {
        "--job-bookmark-option": "job-bookmark-disable",
        "--source_BucketName": importBucket.bucketName,
        "--target_BucketName": exportBucket.bucketName,
      },
      
    })
    
    // const f_pyAssetETL = new Asset(this, "hello-etl", {
    //   path: path.join(__dirname, "assets/hello-etl.py"),
    // })
 
    
    // const jobProps = {
    //   command: {
    //     name: 'glueetl',
    //     pythonVersion: '3',
    //     scriptLocation: f_pyAssetETL.s3ObjectUrl,
    //   },
  
    //   defaultArguments: { },
    //   description: 'etl-job-description',
    //   executionProperty: {
    //     maxConcurrentRuns: 1,
    //   },
    //   glueVersion: '2.0',
    //   maxRetries: 0,
    //   name: 'etl-job',
    //   numberOfWorkers: 2,
    //   role: role.roleArn,
    //   timeout: 180, // minutes
    //   workerType: 'Standard',
      
    //   executable: JobExecutable.pythonEtl({
    //     glueVersion: GlueVersion.V3_0,
    //     pythonVersion: PythonVersion.THREE,
    //     script: Code.fromAsset(path.join(__dirname, "assets/hello-etl.py"))
    //   }),
    // };
      

 
     //create glue database
     /*
    const glue_db = new CfnDatabase(this, 'my-db', {
      catalogId: 'someCatalogId',
      databaseInput :{
        name: 'db-input-name'
      },
    })
    */
    
    //todo - had to manually grant CDK IAM role as "database creators" in lakeformation console.
    //https://github.com/hashicorp/terraform-provider-aws/issues/10251
    // const glue_db = new Database(this, "glue-test-db", {
    //   databaseName: "glue-test-db",
    // })

  }
}

