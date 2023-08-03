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

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityprojectbucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node step tainer landing
steptainerlanding_node1689019246820 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityprojectbucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptainerlanding_node1689019246820",
)

# Script generated for node Renamed keys for privacy filter
Renamedkeysforprivacyfilter_node1689021323334 = ApplyMapping.apply(
    frame=CustomerCurated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("timeStamp", "bigint", "timeStamp", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="Renamedkeysforprivacyfilter_node1689021323334",
)

# Script generated for node privacy filter
privacyfilter_node1689019277595 = Join.apply(
    frame1=steptainerlanding_node1689019246820,
    frame2=Renamedkeysforprivacyfilter_node1689021323334,
    keys1=["serialNumber", "sensorReadingTime"],
    keys2=["right_serialNumber", "timeStamp"],
    transformation_ctx="privacyfilter_node1689019277595",
)

# Script generated for node Drop Fields
DropFields_node1689019302026 = DropFields.apply(
    frame=privacyfilter_node1689019277595,
    paths=[
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
        "right_serialNumber",
    ],
    transformation_ctx="DropFields_node1689019302026",
)

# Script generated for node target
target_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689019302026,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacityprojectbucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="target_node3",
)

job.commit()
