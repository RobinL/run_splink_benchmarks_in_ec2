from splink.datasets import splink_datasets
from splink.duckdb.comparison_library import (
    exact_match,
    jaro_winkler_at_thresholds,
    levenshtein_at_thresholds,
)
from splink.duckdb.linker import DuckDBLinker


def duckdb_performance(df, max_pairs):
    settings_dict = {
        "probability_two_random_records_match": 0.0001,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.postcode_fake = r.postcode_fake and l.first_name = r.first_name",
            "l.first_name = r.first_name and l.surname = r.surname",
            "l.dob = r.dob and substr(l.postcode_fake,1,2) = substr(r.postcode_fake,1,2)",
            "l.postcode_fake = r.postcode_fake and substr(l.dob,1,3) = substr(r.dob,1,3)",
            "l.postcode_fake = r.postcode_fake and substr(l.dob,4,5) = substr(r.dob,4,5)",
        ],
        "comparisons": [
            jaro_winkler_at_thresholds("first_name"),
            jaro_winkler_at_thresholds("surname"),
            levenshtein_at_thresholds("dob"),
            exact_match("birth_place"),
            levenshtein_at_thresholds("postcode_fake"),
            exact_match("gender"),
            exact_match("occupation"),
        ],
        "retain_matching_columns": False,
        "retain_intermediate_calculation_columns": False,
        "additional_columns_to_retain": ["cluster"],
        "max_iterations": 20,
    }

    linker = DuckDBLinker(df, settings_dict)

    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)

    # blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    # linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    # blocking_rule = "l.dob = r.dob"
    # linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df = linker.predict()


def test_2_rounds_1k_duckdb(benchmark):
    df = splink_datasets.historical_50k
    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "max_pairs": 1e5},
        rounds=2,
        iterations=1,
        warmup_rounds=1,
    )
