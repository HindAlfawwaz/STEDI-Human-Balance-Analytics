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

# Script generated for customerlanding_node1 node S3 bucket
customerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/customerdata/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customerlanding_node1",
)

# Script generated for node Amazon S3
AmazonS3t_node1563816190173 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/accelerometerdata/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3t_node1563816190173",
)

# Script generated for node Join Privacy
JoinPrivacy_node1563816190175 = Join.apply(
    frame1=customerlanding_node1,
    frame2=AmazonS3t_node1563816190173,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinPrivacy_node1563816190175",
)

# Script generated for node Drop Fields
DropFieldst_node1563816190174 = DropFields.apply(
    frame=JoinPrivacy_node1563816190175,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFieldst_node1563816190174",
)

# Script generated for node S3 bucket
S3bucket_node7 = glueContext.write_dynamic_frame.from_options(
    frame=DropFieldst_node1563816190174,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://projectstedibucket/accelerometerdata/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node7",
)

job.commit()
