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
S3bucket_node10 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/customerdata/curated_f/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node10",
)

# Script generated for node Amazon S3
steptrainerdatat_node1563816190177 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/steptrainerdata/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerdatat_node1563816190177",
)

# Script generated for node Join
Joint_node1563816190178 = Join.apply(
    frame1=S3bucket_node10,
    frame2=steptrainerdatat_node1563816190177,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Joint_node1563816190178",
)

# Script generated for node Drop Fields
DropFieldst_node1692030001186 = DropFields.apply(
    frame=Joint_node1563816190178,
    paths=[
        "`.serialNumber`",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "email",
        "phone",
        "birthDay",
    ],
    transformation_ctx="DropFieldst_node1692030001186",
)

# Script generated for node S3 bucket
S3bucket_node30 = glueContext.write_dynamic_frame.from_options(
    frame=DropFieldst_node1692030001186,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://projectstedibucket/steptrainerdata/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node30",
)

job.commit()
