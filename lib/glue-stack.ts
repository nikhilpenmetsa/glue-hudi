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


export class GlueStack extends Stack {

  constructor(scope: Construct, id: string, props: PreReqStackProps) {
    super(scope, id, props);

    //getting values from pre-req stack
    const role = props.glueRoleGrantReadWrite;
    const importBucket = props.rawBucket;
    const processedBucket = props.processedBucket;
    const libraryBucket = props.libraryBucket;
    
    const controlTable = props.controlTable;

    controlTable.grantReadData(role);
    importBucket.grantReadWrite(role);
    processedBucket.grantReadWrite(role);
    libraryBucket.grantReadWrite(role);
    

    const job = new Job(this, "glue-job-asset", {
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V2_0,
        pythonVersion: PythonVersion.THREE,
        script: Code.fromBucket(libraryBucket, "scripts/processData.py"),
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
      jobName: 'MeterMeasurementsHudiProcessingJob',
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

