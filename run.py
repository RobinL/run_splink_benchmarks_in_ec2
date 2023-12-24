import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime

import boto3
from watchtower import CloudWatchLogHandler


def setup_cloudwatch_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    boto3.setup_default_session(region_name="eu-west-2")
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
    args = parser.parse_args()

    # Use the parsed arguments
    max_pairs = args.max_pairs
    run_label = args.run_label

    region_name = "eu-west-2"
    bucket = "robinsplinkbenchmarks"
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    logger = setup_cloudwatch_logging()

    # Run pytest benchmark and log its output
    return_code = run_pytest_benchmark(logger, max_pairs)
    if return_code == 0:
        with open("benchmarking_results.json", "r") as file:
            benchmark_data = json.load(file)

        benchmark_data["custom"] = {"max_pairs": max_pairs, "run_label": run_label}

        with open("benchmarking_results.json", "w") as file:
            json.dump(benchmark_data, file, indent=4)

        benchmark_file_name = f"benchmarking_results_{current_time}.json"

        # Rename the file to include the timestamp
        os.rename("benchmarking_results.json", benchmark_file_name)

        # Specify the folder name where the file should be uploaded
        s3_folder_name = "pytest_benchmark_results"

        # Upload the file with the new name to the specified folder
        upload_file_to_s3(
            bucket_name=bucket,
            file_name=benchmark_file_name,
            folder_path=s3_folder_name,
            logger=logger,
            region_name=region_name,
        )

    else:
        logger.error("pytest benchmark command failed.")
