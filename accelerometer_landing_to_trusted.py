import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

def clean_accel_data():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    accelerometer_df = spark.read.json("s3://accelerometerlanding69/accelerometerlanding/")
    customers_df = spark.read.json("s3://customerlanding69/customertrusted/")

    accel_unique = accelerometer_df
    customers_unique = customers_df

    # Join on user == email
    joined_df = accel_unique.join(
        customers_unique,
        accel_unique.user == customers_unique.email,
        "inner"
    )

    customer_cols_to_drop = [col for col in customers_df.columns if col != "email"]
    accelerometer_trusted_df = joined_df.drop(*customer_cols_to_drop)

    accelerometer_trusted_dyf = DynamicFrame.fromDF(
        accelerometer_trusted_df, glueContext, "accelerometer_trusted_dyf"
    )

    sink = glueContext.getSink(
        path="s3://accelerometerlanding69/accelerometertrusted/",
        connection_type="s3",
        updateBehavior="LOG",  # Creates the table if it doesn't exist
        partitionKeys=[],      
        enableUpdateCatalog=True
    )
    sink.setCatalogInfo(
        catalogDatabase="sensor_data",                  
        catalogTableName="accelerometer_trusted"        
    )
    sink.setFormat("json")  # Format must match what you're writing
    sink.writeFrame(accelerometer_trusted_dyf)

    job.commit()

# Entry point
clean_accel_data()
