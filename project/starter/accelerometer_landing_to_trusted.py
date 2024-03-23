import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Parse the job name from the command line arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize the Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read accelerometer data from the landing zone in S3
S3accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkling-lakes/accelerometer/landing"],
        "recurse": True,
    },
    transformation_ctx="S3accelerometerlanding_node1",
)

# Read customer data from the trusted zone in S3
S3customertrusted_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkling-lakes/customer/trusted"],
        "recurse": True,
    },
    transformation_ctx="S3customertrusted_node2",
)

# Join the accelerometer data with customer data based on user-email matching
JoinCustomer_node3 = Join.apply(
    frame1=S3accelerometerlanding_node1,
    frame2=S3customertrusted_node2,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node3",
)

# Drop specific fields from the joined dataset
DropFields_node4 = DropFields.apply(
    frame=JoinCustomer_node3,
    paths=[
        "serialNumber", "shareWithPublicAsOfDate", "birthDay", "registrationDate", 
        "shareWithResearchAsOfDate", "customerName", "email", "lastUpdateDate", 
        "phone", "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node4",
)

# Write the processed data to the trusted zone for accelerometer data in S3
S3accelerometertrusted_node5 = glueContext.getSink(
    path="s3://sparkling-lakes/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3accelerometertrusted_node5",
)
S3accelerometertrusted_node5.setCatalogInfo(
    catalogDatabase="sparkling_lakes", catalogTableName="accelerometer_trusted"
)
S3accelerometertrusted_node5.setFormat("json")
S3accelerometertrusted_node5.writeFrame(DropFields_node4)

# Commit the job to write the data
job.commit()
