import math
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
        "--num_input_rows", action="store", default="1e4", help="Number of input rows"
    )


@pytest.fixture(scope="session")
def max_pairs(request):
    return request.config.getoption("--max_pairs")


@pytest.fixture(scope="session")
def num_input_rows(request):
    return request.config.getoption("--num_input_rows")


comparisons = [
    levenshtein_at_thresholds("first_name", [2, 5]),
    levenshtein_at_thresholds("middle_name", [2, 5]),
    levenshtein_at_thresholds("last_name", [2, 5]),
    levenshtein_at_thresholds("dob", [1, 2, 4]),
    # distance_in_km_at_thresholds("birth_lat", "birth_lng", [10, 100]),
    exact_match("birth_lat"),
    exact_match("birth_lng"),
    exact_match("occupation"),
    # array_intersect_at_sizes("occupation", 1),
]

br_conditions = [
    ["first_name", "last_name"],
    ["first_name", "middle_name"],
    ["middle_name", "last_name"],
    ["occupation", "dob"],
    ["last_name", "birth_country"],
    ["country_citizenship", "dob"],
]


@pytest.fixture(scope="session")
def linker_cpu_salted(num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    con = duckdb.connect(database=":memory:")

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
    salt = math.ceil(cpu_count / len(br_conditions))
    print(f"Salt used = {salt}")

    brs = [block_on(c, salting_partitions=cpu_count) for c in br_conditions]

    settings_complex = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": brs,
        "comparisons": comparisons,
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "max_iterations": 20,
        "em_convergence": 0.001,
    }

    linker = DuckDBLinker("df", settings_complex, connection=con)

    yield linker


@pytest.fixture(scope="session")
def linker_no_salt(num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    con = duckdb.connect(database=":memory:")

    create_table_sql = f"""
    CREATE TABLE df AS
        SELECT * EXCLUDE (cluster, uncorrupted_record)
        FROM '3m_prepared.parquet'
        LIMIT {num_input_rows}
    """
    con.execute(create_table_sql)

    print(con.execute("select count(*) from df").df())

    print(f"No salting")

    brs = [block_on(c) for c in br_conditions]

    settings_complex = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": brs,
        "comparisons": comparisons,
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "max_iterations": 20,
        "em_convergence": 0.001,
    }

    linker = DuckDBLinker("df", settings_complex, connection=con)

    yield linker


@pytest.fixture(scope="session")
def linker_salt_2(num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    con = duckdb.connect(database=":memory:")

    create_table_sql = f"""
    CREATE TABLE df AS
        SELECT * EXCLUDE (cluster, uncorrupted_record)
        FROM '3m_prepared.parquet'
        LIMIT {num_input_rows}
    """
    con.execute(create_table_sql)

    print(con.execute("select count(*) from df").df())

    print(f"Salt=2")

    brs = [block_on(c, salting_partitions=2) for c in br_conditions]

    settings_complex = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": brs,
        "comparisons": comparisons,
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "max_iterations": 20,
        "em_convergence": 0.001,
    }

    linker = DuckDBLinker("df", settings_complex, connection=con)

    yield linker
