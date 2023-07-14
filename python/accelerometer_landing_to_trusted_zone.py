import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Data
CustomerData_node1689327213758 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://saschas-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerData_node1689327213758",
)

# Script generated for node Accelerometer Data
AccelerometerData_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://saschas-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerData_node1",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1689327258014 = Join.apply(
    frame1=CustomerData_node1689327213758,
    frame2=AccelerometerData_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyJoin_node1689327258014",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1689327013049 = Filter.apply(
    frame=CustomerPrivacyJoin_node1689327258014,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1689327013049",
)

# Script generated for node Drop Fields
DropFields_node1689327321259 = DropFields.apply(
    frame=PrivacyFilter_node1689327013049,
    paths=[
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "lastUpdateDate",
        "phone",
        "email",
        "customerName",
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "timeStamp",
    ],
    transformation_ctx="DropFields_node1689327321259",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://saschas-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1689327321259)
job.commit()
