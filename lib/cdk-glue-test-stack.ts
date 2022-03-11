import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Role, ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import { Bucket, BucketAccessControl, BucketEncryption, BlockPublicAccess  } from 'aws-cdk-lib/aws-s3';
import { Database, Job, JobProps, JobExecutable, GlueVersion, PythonVersion,Code } from '@aws-cdk/aws-glue-alpha';
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import * as path from "path";
import { Table, AttributeType, BillingMode } from 'aws-cdk-lib/aws-dynamodb'
import { PreReqStackProps } from './prereq-stack';

export class CdkGlueTestStack extends Stack {
  constructor(scope: Construct, id: string, props: PreReqStackProps) {
    super(scope, id, props);

    // const role = new Role(this, 'access-glue-avista', {
    //   assumedBy: new ServicePrincipal('glue.amazonaws.com')
    // });
    
    //getting role from pre-req stack
    const role = props.glueRoleGrantReadWrite;
    
    //const gluePolicy = new ManagedPolicy(fromAwsManagedPolicyName("service-role/AWSGlueServiceRole");
    //role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))
    
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
 
    // const importBucket = Bucket.fromBucketName(this, "np-glue-hudi-raw2-id", "np-glue-hudi-raw2")
    // const exportBucket = Bucket.fromBucketName(this, "np-glue-hudi-processed2-id", "np-glue-hudi-processed2")
    // const controlTable = Table.fromTableName(this, "GlueCompactionTable-id", "GlueCompactionTable2")

    const importBucket = props.rawBucket;
    const processedBucket = props.processedBucket;
    const controlTable = props.controlTable;

    // const table_name = "nikhiltest"
    // const controlTable2 = new Table(this, 'gluetable2', {
    //   partitionKey: {name: 'glue_job_name', type: AttributeType.STRING},
    //   sortKey: {name: 'tablename_and_pk', type: AttributeType.STRING},
    //   tableName : table_name,
    //   billingMode: BillingMode.PAY_PER_REQUEST, 
    // })

    // rawBucket.grantReadWrite(role);
    // processedBucket.grantReadWrite(role);
    controlTable.grantReadData(role);
//  controlTable2.grantReadData(role);
    importBucket.grantReadWrite(role);
    processedBucket.grantReadWrite(role);
    
    // console.log("------rawBucket--------",rawBucket)
    //console.log("---------------importBucketName--------------------",importBucket)
    //console.log("---------------done---importBucketName--------------------")



    const job = new Job(this, "glue-job-asset", {
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V3_0,
        pythonVersion: PythonVersion.THREE,
        script: Code.fromAsset(path.join(__dirname, "assets/glue_scripts/compact.py"))
      }),
      role: role,
      jobName: 'a-glue-cdk-job',
      maxRetries: 0,
      maxConcurrentRuns: 1,
      defaultArguments: {
        "--job-bookmark-option": "job-bookmark-disable",
        "--source_BucketName": importBucket.bucketName,
        "--target_BucketName": processedBucket.bucketName,
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


