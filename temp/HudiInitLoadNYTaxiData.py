import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME','curated_bucket'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

yellow_tripdata_schema = StructType([ StructField("vendorid",IntegerType(),True), StructField("tpep_pickup_datetime",TimestampType(),True), StructField("tpep_dropoff_datetime",TimestampType(),True), StructField("passenger_count", IntegerType(), True), StructField("trip_distance", DoubleType(), True), StructField("ratecodeid", IntegerType(), True), StructField("store_and_fwd_flag", StringType(), True), StructField("pulocationid", IntegerType(), True), StructField("dolocationid", IntegerType(), True), StructField("payment_type", IntegerType(), True), StructField("fare_amount", DoubleType(), True), StructField("extra", DoubleType(), True), StructField("mta_tax", DoubleType(), True), StructField("tip_amount", DoubleType(), True), StructField("tolls_amount", DoubleType(), True), StructField("improvement_surcharge", DoubleType(), True), StructField("total_amount", DoubleType(), True), StructField("congestion_surcharge", DoubleType(), True), StructField("pk_col", LongType(), True)])

inputDf = spark.read.schema(yellow_tripdata_schema).option("header", "true").csv("s3://nyc-tlc/trip data/yellow_tripdata_{2018,2019,2020}*.csv").withColumn("pk_col",monotonically_increasing_id() + 1)

commonConfig = {'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'tpep_pickup_datetime', 'hoodie.datasource.write.recordkey.field': 'pk_col', 'hoodie.table.name': 'ny_yellow_trip_data', 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.hive_sync.database': 'default', 'hoodie.datasource.hive_sync.table': 'ny_yellow_trip_data', 'hoodie.datasource.hive_sync.enable': 'true', 'path': 's3://' + args['curated_bucket'] + '/default/ny_yellow_trip_data'}
unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 68, 'hoodie.datasource.write.operation': 'bulk_insert'}

combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}

glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
