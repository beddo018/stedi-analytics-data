import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1679293377762,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node remove_pre_opt_in_data
SqlQuery0 = """
select * from f where timestamp >= shareWithResearchAsOfDate;
"""
remove_pre_opt_in_data_node1679624376050 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"f": ApplyMapping_node2},
    transformation_ctx="remove_pre_opt_in_data_node1679624376050",
)

# Script generated for node Drop Fields
DropFields_node1679626247875 = DropFields.apply(
    frame=remove_pre_opt_in_data_node1679624376050,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1679626247875",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1679626247875,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
