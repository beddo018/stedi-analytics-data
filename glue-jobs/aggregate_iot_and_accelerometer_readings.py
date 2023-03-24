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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Readings Trusted
AccelerometerReadingsTrusted_node1679629078364 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerReadingsTrusted_node1679629078364",
    )
)

# Script generated for node Match IoT data to Accelerometer readings
MatchIoTdatatoAccelerometerreadings_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerReadingsTrusted_node1679629078364,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="MatchIoTdatatoAccelerometerreadings_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=MatchIoTdatatoAccelerometerreadings_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
