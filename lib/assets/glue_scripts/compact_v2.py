###########################n############################################################################
#####Import required libraries############################################################################
###########################n############################################################################
import sys
import os
import json
from datetime import datetime
from dateutil import tz

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, upper, lower, current_timestamp, explode, to_date, \
    split, unix_timestamp
from pyspark.sql.types import *

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import concurrent.futures
from time import sleep

import boto3
from botocore.exceptions import ClientError, ParamValidationError

###########################n############################################################################
#####Variables Initilization############################################################################
###########################n############################################################################

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'Environment', 'SandboxNumber'])


spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Set Time zione to PST
to_zone = tz.gettz('US/Pacific')

################ Extract Job Parameters#########################
jobName = args['JOB_NAME'].lower()
environment = args['Environment'].lower()
sandboxNumber = args['SandboxNumber']

logger = glueContext.get_logger()

logger.info('Fetching configuration.')
region = os.environ['AWS_DEFAULT_REGION']


# *************Glue Client*******************
glueClient = boto3.client('glue')


####################### Setting Source and Target bucket names######################################
curatedS3BucketName = 'mycuratedbuckets3'
rawS3BucketName = 'myrawbuckets3'

log_info_file_name = 'logs/{}/info_{}.json'.format(jobName, str(
    datetime.now().astimezone(to_zone).strftime('%Y-%m-%d %H:%M:%S.%f')))

controlFileLocation = "s3://myrawbuckets3/control_file.csv"


dropColumnList = ['db', 'op', 'schema_name', 'transaction_id', 'seq_by_pk']


def main():
    try:
        output_log_info = ""
        ########################################################################################################
        ##### Load Control File ###################################################################################
        ########################################################################################################
        ctrl_schema = StructType([
                                  StructField("db_name", StringType(), True),
                                  StructField("schema_name", StringType(), True),
                                  StructField("table_name", StringType(), True),
                                  StructField("primary_key", StringType(), True),
                                  StructField("partition_key", StringType(), True),
                                  StructField("hudi_storage_type", StringType(), True),
                                  StructField("glue_job_name", StringType(), True),
                                  StructField("dms_full_load_partitioned", StringType(), True),
                                  StructField("hudi_bulkinsert_shuffle_parallelism", IntegerType(), True),
                                  StructField("hudi_upsert_shuffle_parallelism", IntegerType(), True),
                                  StructField("s3_to_redshift", StringType(), True),
                                  StructField("cdc_split_upsert", StringType(), True),
                                  StructField("redshift_timestamp_cols", StringType(), True)
                                  ]
                                 )

        ctrlDf = spark.read.schema(ctrl_schema).option("header", "true").csv(controlFileLocation) \
            .withColumn('db_name', lower(col('db_name'))) \
            .withColumn('schema_name', lower(col('schema_name'))) \
            .withColumn('table_name', lower(col('table_name'))) \
            .withColumn('primary_key', lower(col('primary_key'))) \
            .withColumn('partition_key', lower(col('partition_key'))) \
            .withColumn('hudi_storage_type', lower(col('hudi_storage_type'))) \
            .withColumn('glue_job_name', lower(col('glue_job_name'))) \
            .withColumn('dms_full_load_partitioned', lower(col('dms_full_load_partitioned'))) \
            .withColumn('cdc_split_upsert', lower(col('cdc_split_upsert')))

        ctrlRecsList = ctrlDf.select('db_name', 'schema_name', 'table_name', 'primary_key', 'partition_key',
                                     'hudi_storage_type', 'dms_full_load_partitioned',
                                     'hudi_bulkinsert_shuffle_parallelism', 'hudi_upsert_shuffle_parallelism',
                                     'cdc_split_upsert') \
            .filter("glue_job_name = " + "'" + jobName + "'") \
            .distinct() \
            .rdd.map(lambda row: row.asDict()) \
            .collect()

        print('--------------- ctrlRecsList Count: {} ---------------'.format(len(ctrlRecsList)))

        if len(ctrlRecsList) > 0:
            ctrlRecsToProcessList = []
            for ctrlRec in ctrlRecsList:
                logger.info('Looping for ' + ctrlRec['schema_name'] + '.' + ctrlRec['table_name'])
                print('-----------Looping for {}.{} -----------'.format(ctrlRec['schema_name'], ctrlRec['table_name']))

                # *****************Setting Global Variables *****************************
                dbName = ctrlRec['db_name']
                schemaName = ctrlRec['schema_name']
                glueDbName = ('dl_' + dbName + '_' + schemaName).lower()
                tableNameCatalogCheck = ''
                tableName = ctrlRec['table_name']

                ###########################n############################################################################
                #####Create Glue Database if it does not exists#########################################################
                ###########################n############################################################################
                logger.info('Create Glue Datacatalog if it does not exists.')
                # print('Create Glue Datacatalog if it does not exists.')
                try:
                    response = glueClient.create_database(DatabaseInput={
                        'Name': glueDbName,
                        'Description': 'Database ' + glueDbName + ' created by Glue Compaction Job.'
                    }
                    )

                    isGlueDbCreationSuccess = True
                    print('{} : Created Glue Db.'.format(ctrlRec['table_name']))
                except ClientError as e:
                    if e.response['Error']['Code'] == 'AlreadyExistsException':
                        isGlueDbCreationSuccess = True
                        logger.info('Glue Db Already Exists')
                        print('{} : Glue Db Already Exist.'.format(ctrlRec['table_name']))
                    else:
                        logger.info('Error creating Glue Database:')
                        print('Error creating Glue Database:', e.response['Error']['Code'])
                        isGlueDbCreationSuccess = False

                if isGlueDbCreationSuccess == True:
                    result = process_raw_data(ctrlRec)
                else:
                    log_message = 'Glue GB : {} does not exist neither got created'.format(glueDbName)
                    print(log_message)

        else:
            log_message = 'No tables setup in Glue Control file for {} and {}'.format(jobName, environment)
            print(log_message)

        job.commit()
        print("Job commited successfully")
    except Exception as ex:
        print("gluecompactionjoberror: main method failed with exception: " + str(ex))
        raise ex


# *************Method to construct log object******************************************
def process_raw_data(ctrlRec):
    try:
        print('{} : Started process_raw_data.'.format(ctrlRec['table_name']))
        ########################## Variables Initilization #############################
        dbName = ctrlRec['db_name']
        schemaName = ctrlRec['schema_name']
        tableName = ctrlRec['table_name']
        # glueDbName = ('dl_' + dbName + '_' + schemaName).lower()
        glueDbName = ('dl_' + dbName + '_' + schemaName).lower()
        tableNameCatalogCheck = ''

        isTableExists = False
        isPrimaryKey = False
        isCompositePk = False
        primaryKey = ''
        isPartitionKey = False
        isCompositePartitionKey = False
        partitionKey = ''
        hudiStorageType = ''

        # print(
        #     'Processing records in {} bucket for dbName: {} , schemaName: {} , and tableName: {}. Glue DB name is : {}.'.format(
        #         rawS3BucketName, dbName, schemaName, tableName, glueDbName))

        ##########################Check for PK#############################
        if not ctrlRec['primary_key'] is None:
            isPrimaryKey = True
            primaryKey = ctrlRec['primary_key'].replace(';', ',')
            # verify for Composite Pk
            if (',' in primaryKey):
                isCompositePk = True

        ##########################Check for Partition Key###################
        if not ctrlRec['partition_key'] is None:
            isPartitionKey = True
            partitionKey = ctrlRec['partition_key'].replace(';', ',')
            # verify for multi paritionkey
            if (',' in partitionKey):
                isCompositePartitionKey = True

            #########So setting dummy partition key: TABLE_NAME column##############
        # if (isCompositePk == True and isPartitionKey == False):
        if (isPartitionKey == False):
            isPartitionKey = True
            partitionKey = 'table_name'

        ##########################Check for hudi storage type###################
        if not ctrlRec['hudi_storage_type'] is None:
            hudiStorageType = ctrlRec['hudi_storage_type']

        if (hudiStorageType == 'mor'):
            tableNameCatalogCheck = tableName + '_ro'  # Assumption is that if _ro table exists then _rt table will also exist. Hence we are checking only for _ro.
        else:
            tableNameCatalogCheck = tableName  # The default config in the CF template is CoW. So assumption is that if the user hasn't explicitly requested to create MoR storage type table then we will create CoW tables. Again, if the user overwrites the config with any value other than 'MoR' we will create CoW storage type tables.

        ########################################################################################################
        #####Check if table exsits in Glue Data Catalog#########################################################
        ########################################################################################################
        try:
            # print('glueDbName:', glueDbName)
            glueClient.get_table(DatabaseName=glueDbName, Name=tableNameCatalogCheck)
            print('{} : Table exists in Glue Data Catalog.'.format(ctrlRec['table_name']))
            isTableExists = True
            logger.info(dbName + '.' + tableNameCatalogCheck + ' exists.')
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                isTableExists = False
                logger.info(dbName + '.' + tableNameCatalogCheck + ' does not exist. Table will be created.')
                print('{} : Table will be created in Glue Data Catalog.'.format(ctrlRec['table_name']))

        ########################################################################################################
        #####Build Raw S3 bucket Path List to read data#########################################################
        ########################################################################################################
        if ctrlRec['dms_full_load_partitioned'] == 'yes':
            print('{} : In dms_full_load_partitioned.'.format(ctrlRec['table_name']))
            rawBucketS3PathsList = [
                's3://' + rawS3BucketName + '/' + dbName + '/' + schemaName + '/' + tableName + '_drop' +
                jobName.partition("drop")[2] + '/',
                's3://' + rawS3BucketName + '/' + dbName + '/' + schemaName.upper() + '/' + tableName.upper() + '_DROP' +
                jobName.partition("drop")[2] + '/'
            ]

            isTableExists = False  # added here so that all dms_full_load_partitioned jobs are loaded as FULL load/bulkinsert..
        else:
            rawBucketS3PathsList = [
                's3://' + rawS3BucketName + '/' + dbName + '/' + schemaName + '/' + tableName + '/',
                's3://' + rawS3BucketName + '/' + dbName + '/' + schemaName.upper() + '/' + tableName.upper() + '/'
            ]

        ########################################################################################################
        #####Get records from raw bucket#######################################################################
        ########################################################################################################
        # inputDyf = glueContext.create_dynamic_frame_from_options(connection_type='s3',
        #                                                          connection_options={'paths': rawBucketS3PathsList,
        #                                                                              'groupFiles': 'none',
        #                                                                              'recurse': True},
        #                                                          format='parquet',
        #                                                          transformation_ctx=tableName)
        inputDyf = glueContext.create_dynamic_frame.from_options(
                                                        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
                                                        connection_type="s3",
                                                        format="csv",
                                                        connection_options={
                                                            "paths": rawBucketS3PathsList,
                                                            "recurse": True,
                                                        },
                                                        transformation_ctx=tableName
                                                    )
        inputStgDf = inputDyf.toDF()
        inputStgDf.printSchema()
        inputStgDf.persist()  # persist this dataframe to avoid reading from raw S3 multiple times.


        if not inputStgDf.rdd.isEmpty():

            ########################################################################################################
            #####Rename columns to lowercase########################################################################
            ########################################################################################################
            for colName in inputStgDf.columns:
                inputStgDf = inputStgDf.withColumnRenamed(colName, colName.lower())

            print('{} : inputStgDf Partitions count: {}, after reading from S3 bucket for CDCs.'.format(
                ctrlRec['table_name'],
                inputStgDf.rdd.getNumPartitions()
            )
            )
            # inputStgDf_Part_cnt = inputStgDf.select(spark_partition_id().alias("partitionId")).groupBy("partitionId").count()
            # inputStgDf_Part_cnt.show(n=600000)
            print('{} : Records found in Raw bucket to load into Curated Bucket.'.format(ctrlRec['table_name']))

            if (isTableExists):
                inputStgDf.createOrReplaceTempView("inputStgDf_T")
                ################################################################################################
                ########Build Raw CDC Query and DF Dynamically##################################################
                ################################################################################################
                rawCdcQ = """
                         SELECT *
                           FROM (
                         	    	SELECT ROW_NUMBER() OVER(PARTITION BY {str1}  ORDER BY transaction_id DESC ) seq_by_pk, tab.*
                         		      FROM inputStgDf_T tab
                         		) seq_by_pk_q
                         WHERE seq_by_pk = 1
                         """.format(str1=primaryKey)

                print('{} : In Build Raw CDC Query and DF.'.format(ctrlRec['table_name']))

                inputStgNoDupsDf = spark.sql(rawCdcQ)
                print('{} : inputStgNoDupsDf Partitions count:{}, after window query.'.format(ctrlRec['table_name'],
                                                                                              inputStgNoDupsDf.rdd.getNumPartitions()))

                # inputDf = inputStgNoDupsDf.withColumn('update_ts_dms', to_timestamp(col('update_ts_dms')))
                inputDf = inputStgNoDupsDf
                print(
                    '{} : inputDf Partitions count:{}, after window query'.format(
                        ctrlRec['table_name'], inputDf.rdd.getNumPartitions()))

            else:  # means initial or full load
                # inputDf = inputStgDf.withColumn('update_ts_dms', to_timestamp(col('update_ts_dms')))
                inputDf = inputStgDf
                print(
                    '{} : inputDf Partitions count:{}, in full load'.format(
                        ctrlRec['table_name'], inputDf.rdd.getNumPartitions()))

                print('{} : In build FULL Load DF.'.format(ctrlRec['table_name']))

            ########################################################################################################
            #####Set Target Path###############################################################################
            ########################################################################################################
            targetPath = 's3://' + curatedS3BucketName + '/' + dbName + '/' + schemaName + '/' + tableName
            # print('Got records from raw bucket:{}'.format(inputDf.count()))

            ########################################################################################################
            #####Hudi Config Settings###############################################################################
            ########################################################################################################
            morConfig = {'hoodie.datasource.write.storage.type': 'MERGE_ON_READ',
                         'hoodie.compact.inline': 'false',
                         'hoodie.compact.inline.max.delta.commits': 20,
                         'hoodie.parquet.small.file.limit': 0}

            commonConfig = {'className': 'org.apache.hudi',
                            'hoodie.datasource.hive_sync.use_jdbc': 'false',
                            # 'hoodie.datasource.write.precombine.field': 'update_ts_dms',
                            'hoodie.datasource.write.recordkey.field': primaryKey,
                            'hoodie.table.name': tableName,
                            'hoodie.consistency.check.enabled': 'true',
                            'hoodie.datasource.hive_sync.database': glueDbName,
                            'hoodie.datasource.hive_sync.table': tableName,
                            'hoodie.datasource.hive_sync.enable': 'true',
                            'hoodie.datasource.hive_sync.support_timestamp': 'true',
                            'hoodie.datasource.hive_sync.mode': 'hms'
                            }

            multiPkConfig = {
                'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator'}  # added to support composite PK...#deviation

            #   'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            #   'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.HiveStylePartitionValueExtractor',

            #   'hoodie.datasource.hive_sync.partition_fields': partitionKey}

            partitionDataConfig = {'hoodie.datasource.write.partitionpath.field': partitionKey,
                                   'hoodie.datasource.hive_sync.partition_fields': partitionKey,
                                   'hoodie.datasource.write.hive_style_partitioning': 'true',
                                   'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.HiveStylePartitionValueExtractor',
                                   'hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled': 'true'
                                   }

            unpartitionDataConfig = {
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
                'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}

            incrementalConfig = {'hoodie.upsert.shuffle.parallelism': ctrlRec['hudi_upsert_shuffle_parallelism'],  # 2,
                                 'hoodie.datasource.write.operation': 'upsert',
                                 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                                 'hoodie.cleaner.commits.retained': 10}

            insertConfig = {'hoodie.upsert.shuffle.parallelism': ctrlRec['hudi_upsert_shuffle_parallelism'],  # 2,
                            'hoodie.datasource.write.operation': 'insert'
                            }

            initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': ctrlRec['hudi_bulkinsert_shuffle_parallelism'],
                              # 1500,
                              'hoodie.datasource.write.operation': 'bulk_insert',
                              'hoodie.parquet.writelegacyformat.enabled': 'true',
                              'hoodie.parquet.outputtimestamptype': 'TIMESTAMP_MICROS'
                              }

            deleteDataConfig = {
                'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.EmptyHoodieRecordPayload'}

            print('{} : Configured Hudi Settings.'.format(ctrlRec['table_name']))
            ########################################################################################################
            #####Load Data into Curated Bucket######################################################################
            ########################################################################################################
            if (hudiStorageType == 'mor'):
                commonConfig = {**commonConfig, **morConfig}
                logger.info('mor config appended to commonConfig.')
                print('{} : mor config appended to commonConfig.'.format(ctrlRec['table_name']))

            combinedConf = {}
            if (isPrimaryKey):
                logger.info('Going the Hudi way.')
                if (isTableExists):
                    logger.info('Incremental load.')
                    # glueCdcSplitUpsert = 'yes'
                    ########################################################################################################
                    #####Process upsert-Split-Inserts#######################################################################
                    #####Set ctrlRec['cdc_split_upsert'] to Yes only for table that is huge and no deletes performed########
                    ########################################################################################################
                    # means split inserts and updates; perform bulk insert for I records and upsert for U records.
                    if ctrlRec['cdc_split_upsert'] == 'yes':
                        outputDf_inserted = inputDf.filter("Op = 'I'").drop(*dropColumnList)
                        print('{} : outputDf_inserted Partitions count: {}.'.format(ctrlRec['table_name'],
                                                                                    outputDf_inserted.rdd.getNumPartitions()))

                        if not outputDf_inserted.rdd.isEmpty():  # outputDf.count() > 0:
                            logger.info('upsert-Split-inserts data.')
                            if (isPartitionKey):
                                print('{} : in upsert-split-inserts, yes_tab, yes_pk, yes_part.'.format(
                                    ctrlRec['table_name']))
                                logger.info('Writing to partitioned Hudi table.')
                                # outputDf_inserted = outputDf_inserted.withColumn(partitionKey,
                                #                                                  concat(lit(partitionKey + '='), col(
                                #                                                      partitionKey)))  # Action: Need to figure out for Multi-level partition.
                                if (isCompositePk):
                                    combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig,
                                                    **multiPkConfig}  ##$$$bulk insert using initLoadConfig
                                else:
                                    combinedConf = {**commonConfig, **partitionDataConfig,
                                                    **initLoadConfig}  ##$$$bulk insert using initLoadConfig
                                outputDf_inserted.write.format('org.apache.hudi') \
                                    .options(**combinedConf) \
                                    .mode('Append') \
                                    .save(targetPath)
                            else:
                                print('{} : in upsert-split-inserts, yes_tab, yes_pk, no_part.'.format(
                                    ctrlRec['table_name']))
                                logger.info('Writing to unpartitioned Hudi table.')
                                if (isCompositePk):
                                    combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig,
                                                    **multiPkConfig}  ##$$$bulk insert using initLoadConfig
                                else:
                                    combinedConf = {**commonConfig, **unpartitionDataConfig,
                                                    **initLoadConfig}  ##$$$bulk insert using initLoadConfig
                                outputDf_inserted.write.format('org.apache.hudi') \
                                    .options(**combinedConf) \
                                    .mode('Append') \
                                    .save(targetPath)

                            print(
                                '{} : in upsert-split-inserts, yes_tab, yes_pk, yes_part. Completed bulk insert, outputDf_inserted df to S3 bucket'.format(
                                    ctrlRec['table_name']))

                    ########################################################################################################
                    #####Process upsert-noSplit or upsert-Split-updates####################################################
                    ########################################################################################################
                    # means split inserts and updates; perform bulk insert for I records and upsert for U records
                    if ctrlRec['cdc_split_upsert'] == 'yes':
                        outputDf = inputDf.filter("Op = 'U'").drop(*dropColumnList)
                        print('{} : outputDf Partitions count: {}.'.format(ctrlRec['table_name'],
                                                                           outputDf.rdd.getNumPartitions()))
                        message = 'split-updates'
                    else:
                        # means pick I and U (inserted and updated) records
                        outputDf = inputDf.filter("Op != 'D'").drop(*dropColumnList)
                        print('{} : outputDf Partitions count: {}.'.format(ctrlRec['table_name'],
                                                                           outputDf.rdd.getNumPartitions()))
                        message = 'noSplit'

                    if not outputDf.rdd.isEmpty():  # outputDf.count() > 0:
                        logger.info('Upserting data.')
                        if (isPartitionKey):
                            print(
                                '{} : in upsert-{}, yes_tab, yes_pk, yes_part.'.format(ctrlRec['table_name'], message))
                            logger.info('Writing to partitioned Hudi table.')
                            # outputDf = outputDf.withColumn(partitionKey, concat(lit(partitionKey + '='), col(
                            #     partitionKey)))  # Action: Need to figure out for Multi-level partition.
                            if (isCompositePk):
                                combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig,
                                                **multiPkConfig}
                            else:
                                combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig}
                            outputDf.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append') \
                                .save(targetPath)
                        else:
                            print('{} : in upsert-{}, yes_tab, yes_pk, no_part.'.format(ctrlRec['table_name'], message))
                            logger.info('Writing to unpartitioned Hudi table.')
                            if (isCompositePk):
                                combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig,
                                                **multiPkConfig}
                            else:
                                combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
                            outputDf.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append') \
                                .save(targetPath)

                        print(
                            '{} : in upsert-{}, yes_tab, yes_pk, no_part. Completed upsert, outputDf to S3 bucket'.format(
                                ctrlRec['table_name'], message))

                    ########################################################################################################
                    #####Processes Deletes##D records#######################################################################
                    ########################################################################################################
                    outputDf_deleted = inputDf.filter("Op = 'D'").drop(*dropColumnList)
                    if not outputDf_deleted.rdd.isEmpty():  # outputDf_deleted.count() > 0:
                        logger.info('Some data got deleted.')
                        if (isPartitionKey):
                            print('{} : in delete: yestab, yespk, yespart.'.format(ctrlRec['table_name']))
                            logger.info('Deleting from partitioned Hudi table.')
                            # outputDf_deleted = outputDf_deleted.withColumn(partitionKey, concat(lit(partitionKey + '='),
                            #                                                                     col(partitionKey)))
                            if (isCompositePk):
                                combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig,
                                                **deleteDataConfig, **multiPkConfig}
                            else:
                                combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig,
                                                **deleteDataConfig}
                            outputDf_deleted.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append') \
                                .save(targetPath)
                        else:
                            print('{} : in delete: yestab, yespk, nopart.'.format(ctrlRec['table_name']))
                            logger.info('Deleting from unpartitioned Hudi table.')
                            combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig,
                                            **deleteDataConfig}
                            outputDf_deleted.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append') \
                                .save(targetPath)

                else:
                    outputDf = inputDf.drop(*dropColumnList)
                    if not outputDf.rdd.isEmpty():  # if outputDf.count() > 0:
                        logger.info('Inital load.')
                        if (isPartitionKey):
                            print('{} : in inital_load, no_tab, yes_pk, yes_part.'.format(ctrlRec['table_name']))
                            logger.info('Writing to partitioned Hudi table.')
                            # outputDf = outputDf.withColumn(partitionKey,
                            #                               concat(lit(partitionKey + '='), col(partitionKey)))
                            if (isCompositePk):
                                combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig,
                                                **multiPkConfig}
                            else:
                                combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig}
                            outputDf.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append' if ctrlRec['dms_full_load_partitioned'] == 'yes' else 'Overwrite') \
                                .save(targetPath)
                        else:
                            print('{} : in inital_load, no_tab, yes_pk, no_part.'.format(ctrlRec['table_name']))
                            logger.info('Writing to unpartitioned Hudi table.')
                            if (isCompositePk):
                                combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig,
                                                **multiPkConfig}
                            else:
                                combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
                            outputDf.write.format('org.apache.hudi') \
                                .options(**combinedConf) \
                                .mode('Append' if ctrlRec['dms_full_load_partitioned'] == 'yes' else 'Overwrite') \
                                .save(targetPath)

            else:
                if (isPartitionKey):
                    logger.info('Writing to partitioned glueparquet table.')
                    print('{} : in bad, no_ok, yes_part.'.format(ctrlRec['table_name']))
                    sink = glueContext.getSink(connection_type='s3', path=targetPath, enableUpdateCatalog=True,
                                               updateBehavior='UPDATE_IN_DATABASE', partitionKeys=[partitionKey])
                else:
                    print('{} : in bad, no_ok, no_part.'.format(ctrlRec['table_name']))
                    logger.info('Writing to unpartitioned glueparquet table.')
                    sink = glueContext.getSink(connection_type='s3', path=targetPath, enableUpdateCatalog=True,
                                               updateBehavior='UPDATE_IN_DATABASE')
                sink.setFormat('glueparquet')
                sink.setCatalogInfo(catalogDatabase=dbName, catalogTableName=tableName)
                outputDyf = DynamicFrame.fromDF(inputDf.drop(*dropColumnList), glueContext, 'outputDyf')
                sink.writeFrame(outputDyf)

        inputStgDf.unpersist()  # unpersist dataframe from memory.
        print('{} : process_raw_data executed and returning control to main.'.format(ctrlRec['table_name']))
        return ('{} : process_raw_data Function executed sucessfully.'.format(ctrlRec['table_name']))
    except Exception as ex:
        print(
            '{} : gluecompactionjoberror: process_raw_data method failed with exception:{}'.format(
                ctrlRec['table_name'],
                str(ex)))
        # return ('{} : process_raw_data Function execution FAILED.'.format(ctrlRec['table_name']))
        raise ex


if __name__ == "__main__":
    main()
