import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# This version matches iot data's timestamp to that of trusted accelerometer reading data instead of faulty customer data.

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1679633908949 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1679633908949",
)

# Script generated for node step_trainer_raw
step_trainer_raw_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_raw",
    transformation_ctx="step_trainer_raw_node1",
)

# Script generated for node remove_trainer_data_for_non_curated_customers
step_trainer_raw_node1DF = step_trainer_raw_node1.toDF()
accelerometer_trusted_node1679633908949DF = (
    accelerometer_trusted_node1679633908949.toDF()
)
remove_trainer_data_for_non_curated_customers_node2 = DynamicFrame.fromDF(
    step_trainer_raw_node1DF.join(
        accelerometer_trusted_node1679633908949DF,
        (
            step_trainer_raw_node1DF["sensorreadingtime"]
            == accelerometer_trusted_node1679633908949DF["timestamp"]
        ),
        "leftsemi",
    ),
    glueContext,
    "remove_trainer_data_for_non_curated_customers_node2",
)

# Script generated for node drop_accelerometer_data
drop_accelerometer_data_node1679542284477 = DropFields.apply(
    frame=remove_trainer_data_for_non_curated_customers_node2,
    paths=["user", "x", "timestamp", "y", "z"],
    transformation_ctx="drop_accelerometer_data_node1679542284477",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=drop_accelerometer_data_node1679542284477,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/trainer_data/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
