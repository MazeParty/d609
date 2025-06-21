import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

def machine_learning_to_curated():
        
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
            .appName("Machine Learning Curated Transformation") \
            .getOrCreate()

    step_trainer = spark.read.parquet("s3://steptrainerlanding69/steptrainertrusted/")

    accelerometer = spark.read.parquet("s3://accelerometerlanding69/accelerometertrusted/")

    machine_learning_curated = step_trainer.join(accelerometer,
                    step_trainer.sensorReadingTime == accelerometer.timeStamp,
                    "inner")

    machine_learning_curated.write.mode("overwrite").parquet("s3://machinelearning69/machinelearningcurated/")