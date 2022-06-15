import re
import sys
import os
import boto3
from botocore.exceptions import ClientError, ParamValidationError
from boto3.dynamodb.conditions import Key

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, upper, lower, current_timestamp, explode, to_date, \
    split, unix_timestamp
from pyspark.sql.types import *

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'Environment',
    'source_BucketName',
    'target_BucketName',
    'lib_BucketName',
    'control_Table'
    ])

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

jobName = args['JOB_NAME']
curatedS3BucketName = args['target_BucketName']
rawS3BucketName = args['source_BucketName']
controlTableName = args['control_Table']

glueClient = boto3.client('glue')

#Get job control properties from DynamoDB table
def getJobControlProperties():
    
    jobControlProps = {}
    try:
        dynamodbResource = boto3.resource('dynamodb')
        response = dynamodbResource.Table(controlTableName).query(KeyConditionExpression=Key('glue_job_name').eq(jobName))
        jobControlProps = response['Items']
    except Exception as ex:
        print("Exception fectching job control properties: " + str(ex))
        raise ex
    
    return jobControlProps

#Check if there is an existing database in the Glue catalog
def glueDBExists(glueDbName):

    glueDBExists = False
    try:
        response = glueClient.get_database(Name=glueDbName)
        glueDBExists = True
        print("Glue database {} found in default catalog".format(glueDbName))
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print("Glue database {} not found in default catalog".format(glueDbName))
        else:
            print("Exception getting glue catalog database details: " + str(e))

    return glueDBExists

#Create a database in the Glue catalog
def createGlueDB(glueDbName):

    try:
        response = glueClient.create_database(DatabaseInput={
            'Name': glueDbName,
            'Description': 'Database ' + glueDbName + ' created by Glue Compaction Job.'
            }
        )
        print("Glue database {} created".format(glueDbName))
    except ClientError as e:
        print('Error creating Glue Database:', e.response['Error']['Code'])

#Check if a table already exists for the DB in the Glue Catalog
def tableExists(glueDbName,tableNameCatalogCheck):

    tableExists = False
    try:
        glueClient.get_table(DatabaseName=glueDbName, Name=tableNameCatalogCheck)
        tableExists = True
        print('{} : Table exists in Glue Data Catalog.'.format(tableNameCatalogCheck))
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print('Table {} does not exist. It  will be created in during write operations.'.format(tableNameCatalogCheck))

    return tableExists

#Derive and Set additional job processing properties to process data. 
#These additional properties are used to determing write options during processing.
def enrichJobControlProperties(jobControlRec):

    #set composite key if exists
    jobControlRec['isCompositePk'] = False
    jobControlRec['isPrimaryKey'] = False
    if not jobControlRec['primary_key'] is None:
        #isPrimaryKey = True
        jobControlRec['isPrimaryKey'] = True
        primaryKey = jobControlRec['primary_key'].replace(';', ',')
        # verify for Composite Pk
        if (',' in primaryKey):
            isCompositePk = True
            jobControlRec['isCompositePk'] = True


    #set composite partition key if exists
    jobControlRec['isCompositePartitionKey'] = False
    jobControlRec['isPartitionKey'] = False
    if not jobControlRec['partition_key'] is None:
        #isPartitionKey = True
        jobControlRec['isPartitionKey'] = True
        partitionKey = jobControlRec['partition_key'].replace(';', ',')
        # verify for multi paritionkey
        if (',' in partitionKey):
            isCompositePartitionKey = True
            jobControlRec['isCompositePartitionKey'] = True


    #Check for hudi storage type to calculate table name
    jobControlRec['tableNameCatalogCheck'] = jobControlRec['table_name']
    if not jobControlRec['hudi_storage_type'] is None and (jobControlRec['hudi_storage_type'] == 'mor'):
        jobControlRec['tableNameCatalogCheck'] = jobControlRec['table_name'] + '_ro'  # Assumption is that if _ro table exists then _rt table will also exist. Hence we are checking only for _ro.

    #set isInitalLoad property
    jobControlRec['isInitalLoad'] = True #implies table does not exist
    if tableExists(jobControlRec['glueDbName'],jobControlRec['tableNameCatalogCheck']):
        print("Found an existing table in the Glue catalog. Processing will be configured for incremental load")
        jobControlRec['isInitalLoad'] = False
    else:
        print("No existing table found in the Glue catalog. Processing will be configured for initial/full load")

    return jobControlRec

# Create a map with all possible sets of Hudi Configs.
# Values in the maps will be used to build appropriate Hudi configs
def defineGlobalHudiConfigs(jobControlRec):

    hudiConfigs = {}

    morConfig = {
        'hoodie.datasource.write.storage.type': 'MERGE_ON_READ',
        'hoodie.compact.inline': 'false',
        'hoodie.compact.inline.max.delta.commits': 20,
        'hoodie.parquet.small.file.limit': 0
    }
    hudiConfigs['morConfig'] = morConfig

    commonConfig = {
        'className': 'org.apache.hudi',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.write.precombine.field': jobControlRec['precombine_field'],
        'hoodie.datasource.write.recordkey.field': jobControlRec['primary_key'].replace(';', ','),
        'hoodie.table.name': jobControlRec['table_name'] ,
        'hoodie.consistency.check.enabled': 'true',
        'hoodie.datasource.hive_sync.database': jobControlRec['glueDbName'],
        'hoodie.datasource.hive_sync.table': jobControlRec['table_name'] ,
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.support_timestamp': 'true',
        'hoodie.datasource.hive_sync.mode': 'hms'
    }
    hudiConfigs['commonConfig'] = commonConfig

    multiPkConfig = {
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator'
    }
    hudiConfigs['multiPkConfig'] = multiPkConfig

    partitionDataConfig = {
        'hoodie.datasource.write.partitionpath.field': jobControlRec['partition_key'].replace(';', ','),
        'hoodie.datasource.hive_sync.partition_fields': jobControlRec['partition_key'].replace(';', ','),
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.HiveStylePartitionValueExtractor',
        'hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled': 'true'
    }
    hudiConfigs['partitionDataConfig'] = partitionDataConfig

    unpartitionDataConfig = {
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
    }
    hudiConfigs['unpartitionDataConfig'] = unpartitionDataConfig

    incrementalConfig = {
        'hoodie.upsert.shuffle.parallelism': jobControlRec['hudi_upsert_shuffle_parallelism'],
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
        'hoodie.cleaner.commits.retained': 10
    }
    hudiConfigs['incrementalConfig'] = incrementalConfig

    insertConfig = {
        'hoodie.upsert.shuffle.parallelism': jobControlRec['hudi_upsert_shuffle_parallelism'],
        'hoodie.datasource.write.operation': 'insert'
    }
    hudiConfigs['insertConfig'] = insertConfig

    initLoadConfig = {
        'hoodie.bulkinsert.shuffle.parallelism': jobControlRec['hudi_bulkinsert_shuffle_parallelism'],
        'hoodie.datasource.write.operation': 'bulk_insert',
        'hoodie.parquet.writelegacyformat.enabled': 'true',
        'hoodie.parquet.outputtimestamptype': 'TIMESTAMP_MICROS'
    }
    hudiConfigs['initLoadConfig'] = initLoadConfig

    deleteDataConfig = {
        'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.EmptyHoodieRecordPayload'
    }
    hudiConfigs['deleteDataConfig'] = deleteDataConfig

    if not jobControlRec['hudi_storage_type'] is None and (jobControlRec['hudi_storage_type'] == 'mor'):
        hudiConfigs['commonConfig'] = {**hudiConfigs['commonConfig'], ** hudiConfigs['morConfig']}

    return hudiConfigs

# Calculate appropriate Hudi configs for Initial/Full load
def getHudiConfigForInitialLoad(globalHudiConfigs,jobControlRec):
    
    hudiConfigs = {}
    if (jobControlRec['isPartitionKey']):
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['partitionDataConfig'], **globalHudiConfigs['initLoadConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}
    else:
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['unpartitionDataConfig'], **globalHudiConfigs['initLoadConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}

    return hudiConfigs

# Calculate appropriate Hudi configs for Incremental load
def getHudiConfigForIncrementalLoad(globalHudiConfigs,jobControlRec):

    hudiConfigs = {}
    if (jobControlRec['isPartitionKey']):
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['partitionDataConfig'], **globalHudiConfigs['incrementalConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}
    else:
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['unpartitionDataConfig'], **globalHudiConfigs['incrementalConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}

    return hudiConfigs

# Calculate appropriate Hudi configs for deletes
def getHudiConfigForDeletes(globalHudiConfigs,jobControlRec):

    hudiConfigs = {}
    if (jobControlRec['isPartitionKey']):
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['partitionDataConfig'], **globalHudiConfigs['incrementalConfig'], **globalHudiConfigs['deleteDataConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}
    else:
        hudiConfigs = {**globalHudiConfigs['commonConfig'], **globalHudiConfigs['unpartitionDataConfig'], **globalHudiConfigs['incrementalConfig'], **globalHudiConfigs['deleteDataConfig']}
        if (jobControlRec['isCompositePk']):
            hudiConfigs = {**hudiConfigs,**globalHudiConfigs['multiPkConfig']}

    return hudiConfigs


# Process Initial/Full and incremental data from raw bucket to curated bucket
def process_raw_data(jobControlRec):


    #create Glue Database if it does not exist    
    jobControlRec['glueDbName'] = ('dl_' + jobControlRec['db_name'] + '_' + jobControlRec['schema_name']).lower()
    if not glueDBExists(jobControlRec['glueDbName']):
        createGlueDB(jobControlRec['glueDbName'])

    #Enrich job control properties
    jobControlRec = enrichJobControlProperties(jobControlRec)

    #get a map of all possible hudi configuration options.
    globalHudiConfigs = defineGlobalHudiConfigs(jobControlRec)

    rawBucketS3PathsList = [
        's3://' + rawS3BucketName + '/' + jobControlRec['db_name'] + '/' + jobControlRec['schema_name'] + '/' + jobControlRec['table_name'] + '/',
        's3://' + rawS3BucketName + '/' + jobControlRec['db_name'] + '/' + jobControlRec['schema_name'].upper() + '/' + jobControlRec['table_name'].upper() + '/'
    ]
    targetPath = 's3://' + curatedS3BucketName + '/' + jobControlRec['db_name'] + '/' + jobControlRec['schema_name'] + '/' + jobControlRec['table_name']

    #Read data from raw bucket
    inputDyf = glueContext.create_dynamic_frame_from_options(connection_type='s3',
                    connection_options={'paths': rawBucketS3PathsList,
                                        'groupFiles': 'none',
                                        'recurse': True},
                    format='parquet',
                    transformation_ctx=jobControlRec['table_name'])
    inputStgDf = inputDyf.toDF()
    inputStgDf.printSchema()
    inputStgDf.persist()  # persist this dataframe to avoid reading from raw S3 multiple times.

    print("Count of records being read", inputStgDf.count())

    if not inputStgDf.rdd.isEmpty():
        print('Records found in Raw bucket to load into Curated Bucket. Continue processing')
        for colName in inputStgDf.columns:
            inputStgDf = inputStgDf.withColumnRenamed(colName, colName.lower())
        #print('{} : inputStgDf Partitions count: {}, after reading from S3 bucket.'.format(jobControlRec['table_name'],inputStgDf.rdd.getNumPartitions()))

        if (jobControlRec['isInitalLoad']):
            print('Table {} : Processing Full load.'.format(jobControlRec['table_name']))
            inputDf = inputStgDf
            #print('{} : inputDf Partitions count:{}, in full load'.format(jobControlRec['table_name'], inputDf.rdd.getNumPartitions()))
        else:
            print('Table {} : Processing incremental load - In Build Raw CDC Query and DF.'.format(jobControlRec['table_name']))
            inputStgDf.createOrReplaceTempView("inputStgDf_T")
            #Build Raw CDC Query and DF Dynamically
            rawCdcQ = """
                SELECT *
                FROM (
                        SELECT ROW_NUMBER() OVER(PARTITION BY {str1}  ORDER BY transaction_id DESC ) seq_by_pk, tab.*
                            FROM inputStgDf_T tab
                    ) seq_by_pk_q
                WHERE seq_by_pk = 1
                """.format(str1=jobControlRec['primary_key'].replace(';', ','))
            inputStgNoDupsDf = spark.sql(rawCdcQ)
            #print('{} : inputStgNoDupsDf Partitions count:{}, after window query.'.format(jobControlRec['table_name'],inputStgNoDupsDf.rdd.getNumPartitions()))

            inputDf = inputStgNoDupsDf
            #print('{} : inputDf Partitions count:{}, after window query'.format(jobControlRec['table_name'], inputDf.rdd.getNumPartitions()))


        dropColumnList = ['db', 'op', 'schema_name', 'transaction_id', 'seq_by_pk']
        
        #Process initial/full load
        if(jobControlRec['isInitalLoad']):
            #print('Table {} : Processing Initial/Full load.'.format(jobControlRec['table_name']))
            outputDf = inputDf.drop(*dropColumnList)
            if not outputDf.rdd.isEmpty():  # if outputDf.count() > 0:
                hudiConfigs = getHudiConfigForInitialLoad(globalHudiConfigs,jobControlRec)
                outputDf.write.format('org.apache.hudi').options(**hudiConfigs).mode('Append').save(targetPath)

        #Process incremental load
        else:
            #print('Table {} : Processing incremental load.'.format(jobControlRec['table_name']))
            #Optimize using bulk inserts and updates
            if jobControlRec['cdc_split_upsert'] == 'yes':
                print('Table {} : Optimizing incremental load using bulk inserts and updates'.format(jobControlRec['table_name']))
                outputDf_inserted = inputDf.filter("Op = 'I'").drop(*dropColumnList)
                print('Table {} : Insert Partitions count: {}.'.format(jobControlRec['table_name'], outputDf_inserted.rdd.getNumPartitions()))

                #Handling inserts - Split inserts will be use same hudi config as initial load
                if not outputDf_inserted.rdd.isEmpty():  # outputDf.count() > 0:
                    print('Table {} : Processing bulk inserts.'.format(jobControlRec['table_name']))
                    hudiConfigs = getHudiConfigForInitialLoad(globalHudiConfigs,jobControlRec)
                    outputDf_inserted.write.format('org.apache.hudi').options(**hudiConfigs).mode('Append').save(targetPath)
                    print('Table {} : Completed bulk insert, outputDf_inserted df to S3 bucket'.format(jobControlRec['table_name']))

                #Handling updates
                outputDf = inputDf.filter("Op = 'U'").drop(*dropColumnList)
                print('Table {} : Update Partitions count: {}.'.format(jobControlRec['table_name'],outputDf.rdd.getNumPartitions()))
            else:
                #No optimization bulk insert optmization needed. Process everything except deletes
                outputDf = inputDf.filter("Op != 'D'").drop(*dropColumnList)
                print('Table {} : Upsert/Update Partitions count: {}.'.format(jobControlRec['table_name'],outputDf.rdd.getNumPartitions()))

            #Process updates/upserts.
            if not outputDf.rdd.isEmpty():  # outputDf.count() > 0:
                hudiConfigs = getHudiConfigForIncrementalLoad(globalHudiConfigs,jobControlRec)
                #print("hudiConfigs: getHudiConfigForIncrementalLoad", hudiConfigs)
                print('Table {} : Processing upserts/updates.'.format(jobControlRec['table_name']))
                outputDf.write.format('org.apache.hudi').options(**hudiConfigs).mode('Append').save(targetPath)
                print('Table {} : Completed upserts/updates, outputDf_inserted df to S3 bucket'.format(jobControlRec['table_name']))

            #Process deletes
            outputDf_deleted = inputDf.filter("Op = 'D'").drop(*dropColumnList)
            if not outputDf_deleted.rdd.isEmpty():  # outputDf_deleted.count() > 0:
                print('Table {} : Processing deletes.'.format(jobControlRec['table_name']))
                hudiConfigs = getHudiConfigForDeletes(globalHudiConfigs,jobControlRec)
                outputDf_deleted.write.format('org.apache.hudi').options(**hudiConfigs).mode('Append').save(targetPath)
                print('Table {} : Completed deletes, outputDf_deleted df to S3 bucket'.format(jobControlRec['table_name']))

    #end if not inputStgDf.rdd.isEmpty():
    else:
        print('Table {} : No records found in Raw bucket to process'.format(jobControlRec['table_name']))

    inputStgDf.unpersist()  # unpersist dataframe from memory.
    print('Table {} : Processing completed.'.format(jobControlRec['table_name']))
    #return ('{} : process_raw_data Function executed sucessfully.'.format(jobControlRec['table_name']))


def main():
    #retrieve job processing properties
    jobControlProps = getJobControlProperties()

    if jobControlProps is not None and len(jobControlProps) > 0:
        for jobControlRec in jobControlProps:
            print('Processing {} schema in {} table from job control properties'.format(jobControlRec['schema_name'], jobControlRec['table_name']))
            process_raw_data(jobControlRec)
    else:
        print("No job control properties found for {} in {} DynamoDB table. No processing is attempted".format(jobName,controlTableName))

if __name__ == "__main__":
    main()