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

# Script generated for node step_trainer_raw
step_trainer_raw_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_raw",
    transformation_ctx="step_trainer_raw_node1",
)

# Script generated for node customer_curated
customer_curated_node1679541920073 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1679541920073",
)

# Script generated for node renamed_customer_curated_keys
renamed_customer_curated_keys_node1679541993803 = ApplyMapping.apply(
    frame=customer_curated_node1679541920073,
    mappings=[
        ("serialnumber", "string", "rserialnumber", "string"),
        (
            "sharewithpublicasofdate",
            "long",
            "`(right) sharewithpublicasofdate`",
            "long",
        ),
        ("birthday", "string", "`(right) birthday`", "string"),
        ("registrationdate", "long", "`(right) registrationdate`", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "`(right) sharewithresearchasofdate`",
            "long",
        ),
        ("customername", "string", "`(right) customername`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastupdatedate", "long", "`(right) lastupdatedate`", "long"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "sharewithfriendsasofdate",
            "long",
            "`(right) sharewithfriendsasofdate`",
            "long",
        ),
    ],
    transformation_ctx="renamed_customer_curated_keys_node1679541993803",
)

# Script generated for node remove_trainer_data_for_non_curated_customers
step_trainer_raw_node1DF = step_trainer_raw_node1.toDF()
renamed_customer_curated_keys_node1679541993803DF = (
    renamed_customer_curated_keys_node1679541993803.toDF()
)
remove_trainer_data_for_non_curated_customers_node2 = DynamicFrame.fromDF(
    step_trainer_raw_node1DF.join(
        renamed_customer_curated_keys_node1679541993803DF,
        (
            step_trainer_raw_node1DF["serialnumber"]
            == renamed_customer_curated_keys_node1679541993803DF["rserialnumber"]
        ),
        "leftsemi",
    ),
    glueContext,
    "remove_trainer_data_for_non_curated_customers_node2",
)

# Script generated for node drop_customer_data
drop_customer_data_node1679542284477 = DropFields.apply(
    frame=remove_trainer_data_for_non_curated_customers_node2,
    paths=[
        "`(right) sharewithpublicasofdate`",
        "`(right) birthday`",
        "`(right) registrationdate`",
        "`(right) sharewithresearchasofdate`",
        "`(right) customername`",
        "`(right) email`",
        "`(right) lastupdatedate`",
        "`(right) phone`",
        "`(right) sharewithfriendsasofdate`",
    ],
    transformation_ctx="drop_customer_data_node1679542284477",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=drop_customer_data_node1679542284477,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-srrb/trainer_data/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
