import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689337373193 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1689337373193",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689337384995 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1689337384995",
)

# Script generated for node Join
Join_node1689334863326 = Join.apply(
    frame1=AWSGlueDataCatalog_node1689337373193,
    frame2=AWSGlueDataCatalog_node1689337384995,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689334863326",
)

# Script generated for node Drop Fields
DropFields_node1689335036420 = DropFields.apply(
    frame=Join_node1689334863326,
    paths=["z", "user", "x", "y", "timestamp"],
    transformation_ctx="DropFields_node1689335036420",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://saschas-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1689335036420)
job.commit()
