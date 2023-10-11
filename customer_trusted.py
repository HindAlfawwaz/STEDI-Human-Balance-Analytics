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



#script generated for node S3bucket
customerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://projectstedibucket/customerdata/"],
        "recurse": True,
    },
    transformation_ctx="customerlanding_node1",
)

#script generated for node Filter
Filter_node1692029210885 = Filter.apply(
    frame=customerlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1692029210885",
)

#script generated for node S3 bucket
customertrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1692029210885,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://projectstedibucket/customerdata/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customertrusted_node2",
)

job.commit()
