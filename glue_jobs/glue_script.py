import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_and_print(message):
    logger.info(message)
    print(message)

# Get script arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_bucket'])
log_and_print(f"Arguments: {args}")

# Access the parameters
target_bucket = args['target_bucket']
log_and_print(f"Target bucket: {target_bucket}")

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

log_and_print("Spark and Glue contexts initialized")

try:
    # Load data from AWS Glue Data Catalog
    log_and_print("Loading data from AWS Glue Data Catalog")

    AWSGlueDataCatalog_artist = glueContext.create_dynamic_frame.from_catalog(
        database="etl_data_pipline",
        table_name="dog_data",
        transformation_ctx="AWSGlueDataCatalog_artist"
    )

    log_and_print("Data loaded successfully from Data Catalog")

except Exception as e:
    log_and_print(f"Error: {str(e)}")
    raise

finally:
    job.commit()
    log_and_print("Job committed")
