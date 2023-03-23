import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1679293377762 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-srrb/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1679293377762",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=Accelerometertrusted_node1,
    frame2=CustomerTrustedZone_node1679293377762,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1679294498877 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1679294498877",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1679547131221 = DynamicFrame.fromDF(
    DropFields_node1679294498877.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1679547131221",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1679547131221,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
