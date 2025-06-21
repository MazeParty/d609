import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

def clean_accel_data():
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    job.commit()

    from pyspark.sql import SparkSession

    spark = SparkSession \
            .builder \
            .appName("Accelerometer Trusted") \
            .getOrCreate()


    # Get Accelerometer Landing Zone data
    accelerometer = spark.read.json("s3://accelerometerlanding69/accelerometerlanding/")

    # Get Customer Trusted Zone data
    customers = spark.read.parquet("s3://customerlanding69/customertrusted/")

    # Clean Accelerometer Landing Zone data by doing a join on email and write to Accelerometer Trusted Zone

    customers_unique_values = customers.dropDuplicates(["email"])

    accelerometer_unique_values = accelerometer.dropDuplicates(accelerometer.columns)

    accelerometer_join_customers = accelerometer_unique_values.join(customers_unique_values,
                                    accelerometer_unique_values.user == customers_unique_values.email,
                                    "inner")

    accelerometer_drop_customer_columns = accelerometer_join_customers.drop(*customers.columns)

    # save to accelerometertrusted
    accelerometer_drop_customer_columns.write.mode("overwrite").parquet("s3://accelerometerlanding69/accelerometertrusted/")