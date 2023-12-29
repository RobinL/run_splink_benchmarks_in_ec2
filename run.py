import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from urllib import request
from urllib.error import HTTPError, URLError

import boto3
from watchtower import CloudWatchLogHandler

from benchmarking_utils.cloudwatch import get_metric_data_from_ec2_run

AWS_REGION = "eu-west-2"


def get_ec2_metadata(option):
    try:
        result = subprocess.run(
            ["ec2-metadata", option],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.stdout:
            return result.stdout.split(": ")[1].strip()
        return None
    except subprocess.CalledProcessError as e:
        print(f"Error calling ec2-metadata: {e}")
        return None


def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def setup_cloudwatch_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    boto3.setup_default_session(region_name=AWS_REGION)
    cw_handler = CloudWatchLogHandler(
        log_group="MyTestLogGroup", stream_name="MyTestLogStream"
    )
    logger.addHandler(cw_handler)

    return logger


def run_pytest_benchmark(logger, max_pairs):
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-s",
        "benchmarks/test_splink_50k_synthetic.py",
        "--benchmark-json",
        "benchmarking_results.json",
        "--max_pairs",
        max_pairs,
    ]

    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while True:
        output = process.stdout.readline()
        if output == "" and process.poll() is not None:
            break
        if output:
            logger.info(output.strip())

    rc = process.poll()
    return rc


def upload_file_to_s3(*, bucket_name, file_name, folder_path, logger, region_name):
    s3_client = boto3.client("s3", region_name=region_name)
    s3_file_path = f"{folder_path}/{file_name}"  # Key for S3 includes the folder path
    s3_client.upload_file(file_name, bucket_name, s3_file_path)
    logger.info(
        f"File '{file_name}' uploaded to '{s3_file_path}' in bucket '{bucket_name}'."
    )


if __name__ == "__main__":
    cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)

    metrics_collection_start_time = datetime.utcnow()

    parser = argparse.ArgumentParser(
        description="Run pytest benchmarks with custom parameters."
    )
    parser.add_argument(
        "--max_pairs",
        type=str,
        required=True,
        help="Maximum pairs to process, can be in scientific notation like 1e7.",
    )
    parser.add_argument(
        "--run_label", type=str, required=True, help="A label to describe the run."
    )
    parser.add_argument(
        "--output_bucket",
        type=str,
        required=True,
        help="Name of the S3 bucket to upload results.",
    )

    args = parser.parse_args()

    # Use the parsed arguments
    max_pairs = args.max_pairs
    run_label = args.run_label

    region_name = "eu-west-2"
    output_bucket = args.output_bucket
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    logger = setup_cloudwatch_logging()

    # Run pytest benchmark and log its output
    return_code = run_pytest_benchmark(logger, max_pairs)

    metrics_collection_end_time = datetime.utcnow()

    instance_id = get_ec2_metadata("-i")
    instance_type = get_ec2_metadata("-t")

    response = get_metric_data_from_ec2_run(
        cw_client=cw_client,
        instance=instance_id,
        instance_type=instance_type,
        metrics_collection_start_time=metrics_collection_start_time,
        metrics_collection_end_time=metrics_collection_end_time,
    )

    if return_code == 0:
        with open("benchmarking_results.json", "r") as file:
            benchmark_data = json.load(file)

        custom_data = {}
        custom_data["instance_id"] = instance_id
        custom_data["instance_type"] = instance_type
        custom_data["max_pairs"] = max_pairs
        custom_data["run_label"] = run_label
        custom_data["metrics"] = response

        benchmark_data["custom"] = custom_data

        with open("benchmarking_results.json", "w") as file:
            json.dump(benchmark_data, file, indent=4, default=custom_json_serializer)

        benchmark_file_name = f"benchmarking_results_{current_time}.json"

        # Rename the file to include the timestamp
        os.rename("benchmarking_results.json", benchmark_file_name)

        # Specify the folder name where the file should be uploaded
        s3_folder_name = "pytest_benchmark_results"

        # Upload the file with the new name to the specified folder
        upload_file_to_s3(
            bucket_name=output_bucket,
            file_name=benchmark_file_name,
            folder_path=s3_folder_name,
            logger=logger,
            region_name=region_name,
        )

    else:
        logger.error("pytest benchmark command failed.")
