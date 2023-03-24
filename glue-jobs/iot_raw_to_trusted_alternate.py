import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1679629649257 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1679629649257",
)

# Script generated for node step_trainer_raw
step_trainer_raw_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_raw",
    transformation_ctx="step_trainer_raw_node1",
)

# Script generated for node remove_trainer_data_for_non_curated_customers
step_trainer_raw_node1DF = step_trainer_raw_node1.toDF()
AWSGlueDataCatalog_node1679629649257DF = AWSGlueDataCatalog_node1679629649257.toDF()
remove_trainer_data_for_non_curated_customers_node2 = DynamicFrame.fromDF(
    step_trainer_raw_node1DF.join(
        AWSGlueDataCatalog_node1679629649257DF,
        (
            step_trainer_raw_node1DF["sensorreadingtime"]
            == AWSGlueDataCatalog_node1679629649257DF["timestamp"]
        ),
        "leftsemi",
    ),
    glueContext,
    "remove_trainer_data_for_non_curated_customers_node2",
)

# Script generated for node drop_accelerometer_data
drop_accelerometer_data_node1679542284477 = DropFields.apply(
    frame=remove_trainer_data_for_non_curated_customers_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="drop_accelerometer_data_node1679542284477",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=drop_accelerometer_data_node1679542284477,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/trainer_data/step_trainer_trusted_alternate/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
