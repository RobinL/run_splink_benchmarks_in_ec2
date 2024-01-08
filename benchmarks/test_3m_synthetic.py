import multiprocessing

import duckdb
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.comparison_library import (
    array_intersect_at_sizes,
    distance_in_km_at_thresholds,
    exact_match,
    jaro_winkler_at_thresholds,
    levenshtein_at_thresholds,
)
from splink.duckdb.linker import DuckDBLinker

salts = 20
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
        levenshtein_at_thresholds(
            "first_name", [2, 5], term_frequency_adjustments=True
        ),
        levenshtein_at_thresholds("middle_name", [2, 5]),
        levenshtein_at_thresholds("last_name", [2, 5]),
        levenshtein_at_thresholds("dob", [1, 2, 4]),
        distance_in_km_at_thresholds("birth_lat", "birth_lng", [10, 100]),
        array_intersect_at_sizes("occupation", 1),
    ],
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False,
    "max_iterations": 20,
    "em_convergence": 0.001,
}

settings_simple = {
    "probability_two_random_records_match": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": brs,
    "comparisons": [
        exact_match("first_name"),
        exact_match("middle_name"),
        exact_match("last_name"),
        exact_match("dob"),
        exact_match("birth_lat"),
        exact_match("birth_lng"),
        exact_match("occupation"),
    ],
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False,
    "max_iterations": 10,
    "em_convergence": 0.01,
}


def benchmark_estimate_probability_two_random_records_match(linker):
    linker.estimate_probability_two_random_records_match(
        [
            block_on(["first_name", "last_name", "dob"]),
        ],
        recall=0.8,
    )


def benchmark_estimate_u(max_pairs, linker):
    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)


def benchmark_estimate_parameters_using_expectation_maximisation(linker, cpu_count):
    linker.estimate_parameters_using_expectation_maximisation(
        block_on(["first_name", "last_name"], salting_partitions=cpu_count),
        estimate_without_term_frequencies=True,
    )

    linker.estimate_parameters_using_expectation_maximisation(
        block_on(["dob", "middle_name"], salting_partitions=cpu_count),
        estimate_without_term_frequencies=True,
    )


def benchmark_predict(linker):
    linker.predict(threshold_match_probability=0.9)


def test_estimate_probability_two_random_records_match(benchmark, linker, max_pairs):
    # Without this, tests are not executed in order specified in this file
    _ = max_pairs
    benchmark.pedantic(
        benchmark_estimate_probability_two_random_records_match,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


def test_estimate_u(benchmark, linker, max_pairs):
    print(f"Max pairs = {max_pairs}")
    max_pairs = int(float(max_pairs))
    benchmark.pedantic(
        benchmark_estimate_u,
        kwargs={"max_pairs": max_pairs, "linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


def test_estimate_parameters_using_expectation_maximisation(
    benchmark, linker, max_pairs
):
    _ = max_pairs
    cpu_count = multiprocessing.cpu_count()

    benchmark.pedantic(
        benchmark_estimate_parameters_using_expectation_maximisation,
        kwargs={"linker": linker, "cpu_count": cpu_count},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


def test_predict(benchmark, linker, max_pairs):
    _ = max_pairs
    benchmark.pedantic(
        benchmark_predict,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )
