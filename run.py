import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

import boto3

from benchmarking_utils.cloudwatch import get_metric_data_from_ec2_run


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


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)

    return logger


def run_pytest_benchmark(logger, max_pairs, num_input_rows, aws_region):
    s3_client = boto3.client("s3", region_name=aws_region)
    bucket_name = "robinsplinkbenchmarks"
    object_key = "data/3m_prepared.parquet"
    local_filename = "./3m_prepared.parquet"
    s3_client.download_file(bucket_name, object_key, local_filename)

    command = [
        sys.executable,
        "-m",
        "pytest",
        "-s",
        "benchmarks/test_3m_synthetic.py",
        "--benchmark-json",
        "benchmarking_results.json",
        "--max_pairs",
        max_pairs,
        "--num_input_rows",
        num_input_rows,
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
    metrics_collection_start_time = datetime.utcnow() - timedelta(minutes=1)

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
        "--num_input_rows",
        type=str,
        required=True,
        help="Number of rows in input dataset",
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

    parser.add_argument(
        "--output_folder",
        type=str,
        required=True,
        help="Name of the S3 folder.",
    )

    parser.add_argument(
        "--aws_region",
        type=str,
        required=True,
        help="AWS region for the CloudWatch client.",
    )

    args = parser.parse_args()

    # Use the parsed arguments
    max_pairs = args.max_pairs
    num_input_rows = args.num_input_rows
    run_label = args.run_label

    aws_region = args.aws_region
    output_bucket = args.output_bucket
    output_folder = args.output_folder
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    cw_client = boto3.client("cloudwatch", region_name=aws_region)

    logger = setup_logging()

    # Download test data
    s3_client = boto3.client("s3", region_name=aws_region)
    bucket_name = "robinsplinkbenchmarks"
    object_key = "data/3m_prepared.parquet"
    local_filename = "3m_prepared.parquet"

    # Download the file from S3
    s3_client.download_file(bucket_name, object_key, local_filename)

    # Run pytest benchmark and log its output
    return_code = run_pytest_benchmark(logger, max_pairs, num_input_rows, aws_region)

    metrics_collection_end_time = datetime.utcnow() + timedelta(minutes=1)

    instance_id = get_ec2_metadata("-i")
    instance_type = get_ec2_metadata("-t")

    response = get_metric_data_from_ec2_run(
        cw_client=cw_client,
        instance_id=instance_id,
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
        custom_data["num_input_rows"] = num_input_rows
        custom_data["run_label"] = run_label
        custom_data["metrics"] = response

        benchmark_data["custom"] = custom_data

        with open("benchmarking_results.json", "w") as file:
            json.dump(benchmark_data, file, indent=4, default=custom_json_serializer)

        benchmark_file_name = f"benchmarking_results_{instance_id}_{run_label}.json"

        # Rename the file to include the timestamp
        os.rename("benchmarking_results.json", benchmark_file_name)

        # Upload the file with the new name to the specified folder
        upload_file_to_s3(
            bucket_name=output_bucket,
            file_name=benchmark_file_name,
            folder_path=output_folder,
            logger=logger,
            region_name=aws_region,
        )

    else:
        logger.error("pytest benchmark command failed.")
