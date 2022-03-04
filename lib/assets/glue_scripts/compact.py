import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import boto3
from boto3.dynamodb.conditions import Key

#Creating logger
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)


args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_BucketName',
    'target_BucketName'
    ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger(args["JOB_NAME"])
logger.setLevel(logging.INFO)
#logger.info('in here')


#retrieve job parameters from control table in DynamoDB 
dynamodb_r = boto3.resource('dynamodb') 
compactJobParamItems = dynamodb_r.Table("GlueCompactionTable2").query(
    KeyConditionExpression=Key('glue_job_name').eq("glueJob-datalake-cisdev-etl01")
)

for compactJobParams in compactJobParamItems['Items']:
    logger.info(compactJobParams)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://"+args['source_BucketName']+"/d1_msrtmt_dummy_data.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("measr_comp_id", "string", "measr_comp_id", "string"),
        ("msrmt_dttm", "string", "msrmt_dttm", "string"),
        ("bo_status_cd", "string", "bo_status_cd", "string"),
        ("msrmt_cond_flg", "long", "msrmt_cond_flg", "long"),
        ("msrmt_use_flg", "string", "msrmt_use_flg", "string"),
        ("msrmt_local_dttm", "string", "msrmt_local_dttm", "string"),
        ("msrmt_val", "double", "msrmt_val", "double"),
        ("orig_init_msrmt_id", "string", "orig_init_msrmt_id", "string"),
        ("prev_msrmt_dttm", "string", "prev_msrmt_dttm", "string"),
        ("bus_obj_cd", "string", "bus_obj_cd", "string"),
        ("cre_dttm", "string", "cre_dttm", "string"),
        ("status_upd_dttm", "string", "status_upd_dttm", "string"),
        ("user_edited_flg", "string", "user_edited_flg", "string"),
        ("version", "long", "version", "long"),
        ("last_update_dttm", "string", "last_update_dttm", "string"),
        ("reading_val", "long", "reading_val", "long"),
        ("combined_multiplier", "long", "combined_multiplier", "long"),
        ("reading_cond_flg", "string", "reading_cond_flg", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://"+args['target_BucketName']+"/out/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
