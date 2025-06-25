import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def clean_accel_data():
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    # Set up Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)


    accelerometer = spark.read.json("s3://accelerometerlanding69/accelerometerlanding/")


    customers = spark.read.json("s3://customerlanding69/customertrusted/")


    customers_unique_values = customers.dropDuplicates(["email"])
    accelerometer_unique_values = accelerometer.dropDuplicates(accelerometer.columns)


    accelerometer_join_customers = accelerometer_unique_values.join(
        customers_unique_values,
        accelerometer_unique_values.user == customers_unique_values.email,
        "inner"
    )


    accelerometer_trusted = accelerometer_join_customers.drop(*customers.columns)

    accelerometer_trusted.write.mode("append").json("s3://accelerometerlanding69/accelerometertrusted/")


    job.commit()

# Execute the function
clean_accel_data()
