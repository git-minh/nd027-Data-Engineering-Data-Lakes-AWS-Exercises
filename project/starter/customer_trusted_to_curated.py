import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Get the job name from command line arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Glue context and job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read trusted customer data from S3
S3customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://sparkling-lakes/customer/trusted/"], "recurse": True},
    transformation_ctx="S3customertrusted_node1",
)

# Read trusted steptrainer data from S3
S3steptrainertrusted_node2 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://sparkling-lakes/steptrainer/trusted/"], "recurse": True},
    transformation_ctx="S3steptrainertrusted_node2",
)

# Rename columns in steptrainer data for clarity after joining
Renamesteptrainercolumns_node3 = ApplyMapping.apply(
    frame=S3steptrainertrusted_node2,
    mappings=[
        ("serialNumber", "string", "steptrainerSerialNumber", "string"),
        ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"),
        ("distanceFromObject", "bigint", "steptrainerDistanceFromObject", "bigint"),
    ],
    transformation_ctx="Renamesteptrainercolumns_node3",
)

# Convert DynamicFrames to DataFrames for joining
S3customertrustedDF = S3customertrusted_node1.toDF()
RenamesteptrainercolumnsDF = Renamesteptrainercolumns_node3.toDF()

# Left semi join steptrainer data with customer data based on serialNumber
Joinsteptrainer_node4 = DynamicFrame.fromDF(
    df=S3customertrustedDF.join(
        RenamesteptrainercolumnsDF,
        S3customertrustedDF["serialNumber"] == RenamesteptrainercolumnsDF["steptrainerSerialNumber"],
        "leftsemi"),
    glueContext,
    "Joinsteptrainer_node4",
)

# Drop unnecessary fields coming from steptrainer data
DropFields_node5 = DropFields.apply(
    frame=Joinsteptrainer_node4,
    paths=["steptrainerSerialNumber", "steptrainerDistanceFromObject", "sensorReadingTime"],
    transformation_ctx="DropFields_node5",
)

# Write result to customer curated dataset in S3
S3customercurated_node6 = glueContext.getSink(
    path="s3://sparkling-lakes/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3customercurated_node6",
)
S3customercurated_node6.setCatalogInfo(catalogDatabase="sparkling_lakes", catalogTableName="customer_curated")
S3customercurated_node6.setFormat("json")
S3customercurated_node6.writeFrame(DropFields_node5)

# Commit the job to execute the transformations and write operations
job.commit()
