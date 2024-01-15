import duckdb
import pytest
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.comparison_library import (
    distance_in_km_at_thresholds,
    exact_match,
    jaro_winkler_at_thresholds,
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


@pytest.fixture(scope="session")
def linker(num_input_rows):
    print(f"print num input rows = {num_input_rows}")
    num_input_rows = int(float(num_input_rows))

    con = duckdb.connect(database=":temporary:")

    con.execute("SET temp_directory = 'tmp/'")

    create_table_sql = f"""
    CREATE TABLE df AS
        SELECT * EXCLUDE (cluster, uncorrupted_record)
        FROM '7m_prepared.parquet'
        LIMIT {num_input_rows}
    """
    con.execute(create_table_sql)

    print(con.execute("select count(*) from df").df())

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

    linker = DuckDBLinker("df", settings_complex, connection=con)

    yield linker
