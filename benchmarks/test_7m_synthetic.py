import pytest
from splink.spark.blocking_rule_library import block_on


def benchmark_estimate_probability_two_random_records_match(linker):
    linker.estimate_probability_two_random_records_match(
        [
            block_on(["first_name", "last_name", "dob"]),
        ],
        recall=0.8,
    )


def benchmark_estimate_u(max_pairs, linker):
    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)


def benchmark_estimate_parameters_using_expectation_maximisation(linker):
    linker.estimate_parameters_using_expectation_maximisation(
        block_on(["first_name", "last_name", "occupation"]),
        estimate_without_term_frequencies=True,
    )

    to_drop = []
    for k, v in linker._intermediate_table_cache.items():
        if "__splink__df_comparison_vectors" in k:
            to_drop.append(v)

        if "__splink__agreement_pattern_counts" in k:
            to_drop.append(v)
    [v.drop_table_from_database_and_remove_from_cache() for v in to_drop]

    linker.estimate_parameters_using_expectation_maximisation(
        block_on(["dob", "middle_name"]),
        estimate_without_term_frequencies=True,
    )

    to_drop = []
    for k, v in linker._intermediate_table_cache.items():
        if "__splink__df_comparison_vectors" in k:
            to_drop.append(v)

        if "__splink__agreement_pattern_counts" in k:
            to_drop.append(v)
    [v.drop_table_from_database_and_remove_from_cache() for v in to_drop]


def benchmark_predict(linker):
    linker.predict(threshold_match_probability=0.9)


def benchmark_cluster(linker):
    for k, v in linker._intermediate_table_cache.items():
        if "__splink__df_predict_" in k:
            df_predict = v

    linker.cluster_pairwise_predictions_at_threshold(
        df_predict=df_predict, threshold_match_probability=0.9
    )


@pytest.mark.order(1)
def test_estimate_probability_two_random_records_match(benchmark, linker):
    benchmark.pedantic(
        benchmark_estimate_probability_two_random_records_match,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(2)
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


@pytest.mark.order(3)
def test_estimate_parameters_using_expectation_maximisation(benchmark, linker):
    benchmark.pedantic(
        benchmark_estimate_parameters_using_expectation_maximisation,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(4)
def test_predict(benchmark, linker):
    benchmark.pedantic(
        benchmark_predict,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(5)
def test_cluster(benchmark, linker):
    benchmark.pedantic(
        benchmark_cluster,
        kwargs={"linker": linker},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )
