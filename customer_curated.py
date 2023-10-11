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

# Script generated for node customer_trusted_node5
customer_trusted_node5 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/customerdata/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node5",
)

# Script generated for node accelerometerlandingS3
accelerometerlandingS3_node169 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/accelerometerdata/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlandingS3_node169",
)

# Script generated for node Join Privacy
JoinPrivacy_node123123_h = Join.apply(
    frame1=customer_trusted_node5,
    frame2=accelerometerlandingS3_node169,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinPrivacy_node123123_h",
)

# Script generated for node Drop Fields
DropFields_h_node1563816190176 = DropFields.apply(
    frame=JoinPrivacy_node123123_h,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_h_node1563816190176",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_h_node1563816190176,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://projectstedibucket/customerdata/curated_f/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
