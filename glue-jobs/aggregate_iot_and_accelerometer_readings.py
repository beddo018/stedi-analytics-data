import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Aggregates Accelerometer readings along with Step Trainer IoT data on matching timestamps.

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Readings Trusted
AccelerometerReadingsTrusted_node1679629078364 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerReadingsTrusted_node1679629078364",
    )
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node aggregate_on_timestamps
SqlQuery0 = """
select a.user, s.serialnumber, a.timestamp, a.x, a.y, a.z, s.distancefromobject from s inner join a on (s.sensorreadingtime = a.timestamp) group by user, serialnumber, timestamp, x, y, z, distancefromobject order by timestamp desc;
"""
aggregate_on_timestamps_node1679631109950 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "a": AccelerometerReadingsTrusted_node1679629078364,
        "s": StepTrainerTrusted_node1,
    },
    transformation_ctx="aggregate_on_timestamps_node1679631109950",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=aggregate_on_timestamps_node1679631109950,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
