import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def clean_customer_data():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    LandingCustomerZone_node1 = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://customerlanding69/customerlanding/"],
            "recurse": True,
        },
        transformation_ctx="LandingCustomerZone_node1",
    )

    trusted_zone_data = Filter.apply(
        frame=LandingCustomerZone_node1,
        f=lambda row: row.get("shareWithResearchAsOfDate") not in (0, None),
        transformation_ctx="trusted_zone_data",
    )

    TrustedCustomerZone_node3 = glueContext.getSink(
        path="s3://customerlanding69/customertrusted/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE", 
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="TrustedCustomerZone_node3",
    )
    TrustedCustomerZone_node3.setCatalogInfo(
        catalogDatabase="sensor_data",
        catalogTableName="customer_trusted"
    )
    TrustedCustomerZone_node3.setFormat("json")
    TrustedCustomerZone_node3.writeFrame(trusted_zone_data)

    job.commit()
clean_customer_data()