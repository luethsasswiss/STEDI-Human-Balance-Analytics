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

# Script generated for node Amazon S3
AmazonS3_node1689337827302 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://saschas-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1689337827302",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://saschas-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1689337961854 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1689337827302,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1689337961854",
)

# Script generated for node Drop Fields
DropFields_node1689338704126 = DropFields.apply(
    frame=Join_node1689337961854,
    paths=[
        "customerName",
        "phone",
        "email",
        "birthDay",
        "`.serialNumber`",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689338704126",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://saschas-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1689338704126)
job.commit()
