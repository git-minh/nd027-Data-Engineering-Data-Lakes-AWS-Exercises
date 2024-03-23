import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Get the job name from command-line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()  # Ensure a Spark context is created only once
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read trusted accelerometer data from S3 
S3accelerometertrusted = glueContext.create_dynamic_frame.from_options(
    format_options={'multiline': False},
    connection_type='s3',
    format='json',
    connection_options={'paths': ['s3://sparkling-lakes/accelerometer/trusted/'], 'recurse': True},
    transformation_ctx='S3accelerometertrusted',
)

# Read trusted steptrainer data from S3
S3steptrainertrusted = glueContext.create_dynamic_frame.from_options(
    format_options={'multiline': False},
    connection_type='s3',
    format='json',
    connection_options={'paths': ['s3://sparkling-lakes/steptrainer/trusted/'], 'recurse': True},
    transformation_ctx='S3steptrainertrusted',
)

# Join accelerometer and steptrainer data
joined_data = Join.apply(
    frame1=S3steptrainertrusted,
    frame2=S3accelerometertrusted,
    keys1=['sensorReadingTime'],
    keys2=['timeStamp'],
    transformation_ctx='joined_data',
)

# Drop "sensorReadingTime" field post-join
cleaned_data = DropFields.apply(
    frame=joined_data,
    paths=['sensorReadingTime'],
    transformation_ctx='cleaned_data',
)

# Write cleaned data to curated dataset in S3
curated_sink = glueContext.getSink(
    path='s3://sparkling-lakes/machinelearning/curated/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx='curated_sink',
)
curated_sink.setCatalogInfo(
    catalogDatabase='sparkling_lakes',
    catalogTableName='machine_learning_curated'
)
curated_sink.setFormat('json')
curated_sink.writeFrame(cleaned_data)

# Commit the job to execute the data processing and writing tasks
job.commit()
