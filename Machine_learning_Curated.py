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

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityprojectbucket/accelerometer/trusted"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_trusted_node1",
)

# Script generated for node step tainer Trusted
steptainerTrusted_node1689019246820 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityprojectbucket/step_trainer/trusted"],
        "recurse": True,
    },
    transformation_ctx="steptainerTrusted_node1689019246820",
)

# Script generated for node privacy filter
privacyfilter_node1689019277595 = Join.apply(
    frame1=steptainerTrusted_node1689019246820,
    frame2=Accelerometer_trusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="privacyfilter_node1689019277595",
)

# Script generated for node Drop Fields
DropFields_node1689019302026 = DropFields.apply(
    frame=privacyfilter_node1689019277595,
    paths=[
        "timeStamp",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "sensorReadingTime",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "`.serialNumber`",
        "birthDay",
        "`.timeStamp`",
        "`.shareWithPublicAsOfDate`",
        "`.shareWithResearchAsOfDate`",
        "`.registrationDate`",
        "customerName",
        "`.lastUpdateDate`",
        "`.shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1689019302026",
)

# Script generated for node Machine Learning Curated Zone
MachineLearningCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689019302026,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacityprojectbucket/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCuratedZone_node3",
)

job.commit()
