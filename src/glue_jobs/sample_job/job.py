import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from src.common.s3_utils import S3Helper
from src.common.exceptions import DataValidationError

# Note: 'awsglue' module is only available in AWS Glue environment

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def main():
    try:
        #read inout data
        input_df = glueContext.create_dynamic_frame.from_options(
            "s3",
            {"paths": [args["INPUT_PATH"]]},
            format="csv"
        )

        # Simple transformation - just uppercase a string column
        # Replace with your actual transformation logic
        transformed_df = input_df.apply(lambda x: x.upper())

        #write output
        glueContext.write_dynamic_frame.from_options(
            frame=transformed_df,
            connection_type = "s3",
            connection_options={"path": args['OUTPUT_PATH']},
            format="parquet"
        )

    except Exception as e:
        raise DataValidationError(f"Job failed: {str(e)}")

if __name__ == "__main__":
    main()
    job.commit()




