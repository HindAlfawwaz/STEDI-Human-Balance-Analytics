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
S3bucket_node200 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/accelerometerdata/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node200",
)

# Script generated for node Amazon S3
AmazonS3t_node1563816190169 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/steptrainerdata/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3t_node1563816190169",
)

# Script generated for node Join
Join_nodet1563816190170 = Join.apply(
    frame1=S3bucket_node200,
    frame2=AmazonS3t_node1563816190169,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_nodet1563816190170",
)

# Script generated for node Drop Fields
DropFieldst_node1563816190171  = DropFields.apply(
    frame=Join_nodet1563816190170,
    paths=["user"],
    transformation_ctx="DropFieldst_node1563816190171",
)

# Script generated for node S3 bucket
S3bucket_node1563816190172 = glueContext.write_dynamic_frame.from_options(
    frame=DropFieldst_node1563816190171,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://projectstedibucket/output/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node1563816190172",
)

job.commit()
