import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Role, ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import { Bucket, BucketAccessControl, BucketEncryption, BlockPublicAccess  } from 'aws-cdk-lib/aws-s3';
import { Database, Job, JobProps, JobExecutable, GlueVersion, PythonVersion,Code,ContinuousLoggingProps, ISecurityConfiguration,
PythonSparkJobExecutableProps, WorkerType } from '@aws-cdk/aws-glue-alpha';
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
    const libraryBucket = props.libraryBucket;
    
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
    libraryBucket.grantReadWrite(role);
    
    // console.log("------rawBucket--------",rawBucket)
    //console.log("---------------importBucketName--------------------",importBucket)
    //console.log("---------------done---importBucketName--------------------")



    const job = new Job(this, "glue-job-asset", {
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V2_0,
        pythonVersion: PythonVersion.THREE,
        //script: Code.fromAsset(path.join(__dirname, "assets/glue_scripts/compact_v2.py")),
        script: Code.fromBucket(libraryBucket, "scripts/processData2.py"),
        extraJars: [
          Code.fromBucket(libraryBucket, "jars/hudi-spark-bundle_2.11-0.10.1.jar"),
          Code.fromBucket(libraryBucket, "jars/spark-avro_2.11-2.4.4.jar"),
          Code.fromBucket(libraryBucket, "jars/log4j-web-2.16.0.jar"),]
      }),
      continuousLogging: {
        enabled: true
      },
      enableProfilingMetrics: true,
      role: role,
      jobName: 'MeterMeasurementsHudiCompactionJob',
      maxRetries: 0,
      maxConcurrentRuns: 1,
      defaultArguments: {
        "--job-bookmark-option": "job-bookmark-enable",
        "--source_BucketName": importBucket.bucketName,
        "--target_BucketName": processedBucket.bucketName,
        "--lib_BucketName": libraryBucket.bucketName,
        "--control_Table": controlTable.tableName,
        "--additional-python-modules": "boto3==1.17.39,botocore==1.20.39",
        "--enable-glue-datacatalog": "",
        "--Environment": "demo"
      },
      workerCount: 3,
      workerType: WorkerType.G_1X
      
    })
    
  }
}
