import multiprocessing

from splink.duckdb.blocking_rule_library import block_on


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

    for k, df in linker._intermediate_table_cache.items():
        if "predict" in k:
            tab = df.physical_name
            print(linker.query_sql(f"select count(*) as p_count from {tab}"))
