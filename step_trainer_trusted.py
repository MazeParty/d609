import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, lower

def clean_steptrainer_data():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    # Initialize contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Load data from S3
    customer_df = spark.read.json("s3://customerlanding69/customercurated/output/")
    accelerometer_df = spark.read.json("s3://accelerometerlanding69/accelerometertrusted/")
    step_trainer_df = spark.read.json("s3://steptrainerlanding69/steptrainerlanding/")

    step_trainer_df = step_trainer_df.withColumnRenamed("serialNumber", "st_serialNumber") \
                                     .withColumn("st_serialNumber", trim(lower(col("st_serialNumber"))))

    customer_df = customer_df.withColumn("serialNumber", trim(lower(col("serialNumber")))) \
                             .withColumn("email", trim(lower(col("email"))))

    accelerometer_df = accelerometer_df.withColumn("user", trim(lower(col("user"))))

    # Deduplicate only customer and accelerometer users
    customer_df = customer_df.dropDuplicates(["serialNumber", "email"])
    accel_users_df = accelerometer_df.select("user").dropDuplicates()

    step_customer_join = step_trainer_df.join(
        customer_df,
        step_trainer_df["st_serialNumber"] == customer_df["serialNumber"],
        "inner"
    )

    filtered = step_customer_join.join(
        accel_users_df,
        step_customer_join["email"] == accel_users_df["user"],
        "inner"
    )
    final_step_trainer_only = filtered.select(step_trainer_df.columns)

    final_dyf = DynamicFrame.fromDF(final_step_trainer_only, glueContext, "final_dyf")

    sink = glueContext.getSink(
        path="s3://steptrainerlanding69/steptrainertrusted/output/",
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=[],
        enableUpdateCatalog=True
    )
    sink.setCatalogInfo(
        catalogDatabase="sensor_data",
        catalogTableName="step_trainer_trusted"
    )
    sink.setFormat("json")
    sink.writeFrame(final_dyf)

    job.commit()

# Entry point
clean_steptrainer_data()
