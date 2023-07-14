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
AWSGlueDataCatalog_node1689341217211 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1689341217211",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689341204768 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1689341204768",
)

# Script generated for node Join
Join_node1689341229150 = Join.apply(
    frame1=AWSGlueDataCatalog_node1689341217211,
    frame2=AWSGlueDataCatalog_node1689341204768,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1689341229150",
)

# Script generated for node Drop Fields
DropFields_node1689341246807 = DropFields.apply(
    frame=Join_node1689341229150,
    paths=["user"],
    transformation_ctx="DropFields_node1689341246807",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://saschas-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1689341246807)
job.commit()
