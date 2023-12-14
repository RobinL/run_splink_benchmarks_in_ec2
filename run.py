import logging
from datetime import datetime

import boto3
from watchtower import CloudWatchLogHandler


def setup_cloudwatch_logging():
    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    boto3.setup_default_session(region_name="eu-west-2")

    # Create a CloudWatch log handler with the specified region
    cw_handler = CloudWatchLogHandler(
        log_group="MyTestLogGroup",
        stream_name="MyTestLogStream",
    )

    # Add the CloudWatch log handler to the logger
    logger.addHandler(cw_handler)

    return logger


def create_and_upload_file(bucket_name, file_name, content, logger, region_name):
    # Create a file with the specified content
    with open(file_name, "w") as file:
        file.write(content)

    # Initialize the S3 client with the specified region
    s3_client = boto3.client("s3", region_name=region_name)

    # Upload the file to S3
    s3_client.upload_file(file_name, bucket_name, file_name)
    logger.info(f"File '{file_name}' uploaded to bucket '{bucket_name}'.")


if __name__ == "__main__":
    region_name = "eu-west-2"
    bucket = "robinsplinkbenchmarks"
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"hello_world_{current_time}.txt"
    content = "hi"

    # Setup CloudWatch logging
    logger = setup_cloudwatch_logging()

    # Log a message to CloudWatch
    logger.info("Hello World")

    create_and_upload_file(bucket, file_name, content, logger, region_name)
