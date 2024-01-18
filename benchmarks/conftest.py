import multiprocessing

import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from splink.spark.blocking_rule_library import block_on
from splink.spark.comparison_library import (
    distance_in_km_at_thresholds,
    exact_match,
    jaro_winkler_at_thresholds,
    levenshtein_at_thresholds,
)
from splink.spark.jar_location import similarity_jar_location
from splink.spark.linker import SparkLinker


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


@pytest.fixture(scope="session")
def linker(spark, num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    df = spark.read.parquet("7m_prepared.parquet")

    df = df.drop("cluster", "uncorrupted_record").limit(num_input_rows)

    df.createOrReplaceTempView("df")

    create_table_sql = f"""
        SELECT *
        FROM df
        LIMIT {num_input_rows}
    """

    df = spark.sql(create_table_sql)
    df.createOrReplaceTempView("df")

    print(df.count())

    br_conditions = [
        ["last_name", "occupation"],
        ["first_name", "last_name"],
        ["first_name", "middle_name"],
        ["middle_name", "last_name"],
        ["first_name", "dob"],
        ["first_name", "middle_name"],
        ["last_name", "birth_lat"],
        ["first_name", "birth_lng"],
        ["middle_name", "occupation"],
    ]

    brs = [block_on(c) for c in br_conditions]

    settings_complex = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": brs,
        "comparisons": [
            jaro_winkler_at_thresholds(
                "first_name", [0.9, 0.7], term_frequency_adjustments=True
            ),
            jaro_winkler_at_thresholds("middle_name", [0.9]),
            jaro_winkler_at_thresholds(
                "last_name", [0.9, 0.7], term_frequency_adjustments=True
            ),
            levenshtein_at_thresholds(
                "dob", [1, 2, 4], term_frequency_adjustments=True
            ),
            distance_in_km_at_thresholds("birth_lat", "birth_lng", [10, 100]),
            exact_match("occupation", term_frequency_adjustments=True),
        ],
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "max_iterations": 20,
        "em_convergence": 0.001,
    }

    linker = SparkLinker(df, settings_complex)

    yield linker


@pytest.fixture(scope="session")
def spark():
    conf = SparkConf()
    cpu_count_str = str(multiprocessing.cpu_count())

    conf.set("spark.driver.memory", "24g")
    conf.set("spark.executor.memory", "24g")
    conf.set("spark.driver.memoryOverhead", "4g")
    conf.set("spark.executor.memoryOverhead", "4g")

    conf.set("spark.sql.shuffle.partitions", cpu_count_str)
    conf.set("spark.default.parallelism", cpu_count_str)

    path = similarity_jar_location()
    conf.set("spark.jars", path)

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

    return spark
