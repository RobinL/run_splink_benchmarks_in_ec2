import multiprocessing

import boto3
import duckdb
import pytest
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.comparison_library import (
    array_intersect_at_sizes,
    distance_in_km_at_thresholds,
    exact_match,
    levenshtein_at_thresholds,
)
from splink.duckdb.linker import DuckDBLinker


def pytest_addoption(parser):
    parser.addoption(
        "--max_pairs", action="store", default="1e6", help="Maximum pairs to process"
    )
    parser.addoption(
        "--num_input_rows", action="store", default="1e5", help="Number of input rows"
    )


@pytest.fixture(scope="session")
def max_pairs(request):
    return request.config.getoption("--max_pairs")


@pytest.fixture(scope="session")
def num_input_rows(request):
    return request.config.getoption("--num_input_rows")


@pytest.fixture(scope="session")
def linker(num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    con = duckdb.connect(database=":memory:")

    # con.execute("SET home_directory='/home/ec2-user'")

    # can't read direct from s3 on arm so instead downloaded it using boto3
    # https://github.com/duckdb/duckdb/issues/8035#issuecomment-1819348416
    create_table_sql = f"""
    CREATE TABLE df AS
        SELECT * EXCLUDE (cluster, uncorrupted_record)
        FROM '3m_prepared.parquet'
        LIMIT {num_input_rows}
    """
    con.execute(create_table_sql)

    print(con.execute("select count(*) from df").df())

    cpu_count = multiprocessing.cpu_count()
    print(f"Number of cores = {cpu_count}")
    salts = int(cpu_count / 1)

    brs = [
        block_on(["first_name", "last_name"], salting_partitions=salts),
        block_on(["first_name", "middle_name"], salting_partitions=salts),
        block_on(["middle_name", "last_name"], salting_partitions=salts),
        block_on(["occupation", "dob"], salting_partitions=salts),
        block_on(["last_name", "birth_country"], salting_partitions=salts),
        block_on(["country_citizenship", "dob"], salting_partitions=salts),
    ]
    settings_complex = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": brs,
        "comparisons": [
            levenshtein_at_thresholds("first_name", [2, 5]),
            levenshtein_at_thresholds("middle_name", [2, 5]),
            levenshtein_at_thresholds("last_name", [2, 5]),
            levenshtein_at_thresholds("dob", [1, 2, 4]),
            # distance_in_km_at_thresholds("birth_lat", "birth_lng", [10, 100]),
            exact_match("birth_lat"),
            exact_match("birth_lng"),
            exact_match("occupation"),
            # array_intersect_at_sizes("occupation", 1),
        ],
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "max_iterations": 20,
        "em_convergence": 0.001,
    }

    linker = DuckDBLinker("df", settings_complex, connection=con)

    yield linker
