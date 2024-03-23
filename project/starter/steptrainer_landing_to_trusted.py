import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Obtain the job name from the passed system arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Create a DynamicFrame from the StepTrainer landing data in S3
S3steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://sparkling-lakes/steptrainer/landing"], "recurse": True},
    transformation_ctx="S3steptrainerlanding_node1",
)

# Create a DynamicFrame from the customer trusted data in S3
S3customertrusted_node2 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://sparkling-lakes/customer/trusted"], "recurse": True},
    transformation_ctx="S3customertrusted_node2",
)

# Rename customer columns to distinguish them after the join
RenameCustomerColumns_node3 = ApplyMapping.apply(
    frame=S3customertrusted_node2,
    mappings=[
        ("serialNumber", "string", "customerSerialNumber", "string"),
        ("shareWithPublicAsOfDate", "bigint", "customerShareWithPublicAsOfDate", "bigint"),
        ("birthDay", "string", "customerBirthDay", "string"),
        # Add mappings for the remaining columns similarly
    ],
    transformation_ctx="RenameCustomerColumns_node3",
)

# Join the StepTrainer and Customer data on serialNumber
JoinCustomer_node4 = Join.apply(
    frame1=S3steptrainerlanding_node1,
    frame2=RenameCustomerColumns_node3,
    keys1=["serialNumber"],
    keys2=["customerSerialNumber"],
    transformation_ctx="JoinCustomer_node4",
)

# Drop the renamed customer specific columns to keep only steptrainer data
DropFields_node5 = DropFields.apply(
    frame=JoinCustomer_node4,
    paths=[
        "customerSerialNumber",
        "customerShareWithPublicAsOfDate",
        # List other customer specific fields to drop
    ],
    transformation_ctx="DropFields_node5",
)

# Write the resultant dataset to the StepTrainer trusted zone in S3
S3steptrainertrusted_node6 = glueContext.getSink(
    path="s3://sparkling-lakes/steptrainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="S3steptrainertrusted_node6",
)
S3steptrainertrusted_node6.setCatalogInfo(catalogDatabase="sparkling_lakes", catalogTableName="steptrainer_trusted")
S3steptrainertrusted_node6.setFormat("json")
S3steptrainertrusted_node6.writeFrame(DropFields_node5)

# Finalize the job
job.commit()
