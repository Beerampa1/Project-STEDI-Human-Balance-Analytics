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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1688946516884 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1688946516884",
)

# Script generated for node Join
Join_node1688946525184 = Join.apply(
    frame1=AmazonS3_node1688946516884,
    frame2=S3bucket_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1688946525184",
)

# Script generated for node Drop Fields
DropFields_node1688946536545 = DropFields.apply(
    frame=Join_node1688946525184,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1688946536545",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688946536545,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacityprojectbucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
