import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

def customer_trusted_to_curated():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    accelerometer_df = spark.read.json("s3://accelerometerlanding69/accelerometertrusted/")
    customers_df = spark.read.json("s3://customerlanding69/customertrusted/")

    accelerometer_unique = accelerometer_df.dropDuplicates(["user"])
    customers_unique = customers_df.dropDuplicates(["email"])

    accel_alias = accelerometer_unique.alias("accel")
    cust_alias = customers_unique.alias("cust")

    # Inner join on email == user
    joined_df = cust_alias.join(
        accel_alias,
        cust_alias["email"] == accel_alias["user"],
        "inner"
    )

    customer_columns = customers_df.columns
    final_df = joined_df.select([f"cust.{col}" for col in customer_columns])

    curated_dyf = DynamicFrame.fromDF(final_df, glueContext, "curated_dyf")

    sink = glueContext.getSink(
        path="s3://customerlanding69/customercurated/output/",
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=[],
        enableUpdateCatalog=True
    )
    sink.setCatalogInfo(
        catalogDatabase="sensor_data",
        catalogTableName="customer_curated"
    )
    sink.setFormat("json")
    sink.writeFrame(curated_dyf)

    job.commit()

# Entry point
customer_trusted_to_curated()
