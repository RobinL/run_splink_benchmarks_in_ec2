import multiprocessing

import pytest
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


@pytest.mark.order(1)
def test_salt_2_estimate_probability_two_random_records_match(benchmark, linker_salt_2):
    benchmark.pedantic(
        benchmark_estimate_probability_two_random_records_match,
        kwargs={"linker": linker_salt_2},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(2)
def test_salt_2_estimate_u(benchmark, linker_salt_2, max_pairs):
    print(f"Max pairs = {max_pairs}")
    max_pairs = int(float(max_pairs))
    benchmark.pedantic(
        benchmark_estimate_u,
        kwargs={"max_pairs": max_pairs, "linker": linker_salt_2},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(3)
def test_salt_2_estimate_parameters_using_expectation_maximisation(
    benchmark, linker_salt_2
):
    cpu_count = multiprocessing.cpu_count()

    benchmark.pedantic(
        benchmark_estimate_parameters_using_expectation_maximisation,
        kwargs={"linker": linker_salt_2, "cpu_count": cpu_count},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(4)
def test_salt_2_predict(benchmark, linker_salt_2):
    benchmark.pedantic(
        benchmark_predict,
        kwargs={"linker": linker_salt_2},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(5)
def test_salt_2_cleanup(linker_salt_2):
    for k, df in linker_salt_2._intermediate_table_cache.items():
        if "predict" in k:
            tab = df.physical_name
            print(linker_salt_2.query_sql(f"select count(*) as p_count from {tab}"))
    linker_salt_2._con.close()
    print("closed linker_salt_2 connection")


@pytest.mark.order(6)
def test_cpu_salted_estimate_probability_two_random_records_match(
    benchmark, linker_cpu_salted
):
    benchmark.pedantic(
        benchmark_estimate_probability_two_random_records_match,
        kwargs={"linker": linker_cpu_salted},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(7)
def test_cpu_salted_estimate_u(benchmark, linker_cpu_salted, max_pairs):
    print(f"Max pairs = {max_pairs}")
    max_pairs = int(float(max_pairs))
    benchmark.pedantic(
        benchmark_estimate_u,
        kwargs={"max_pairs": max_pairs, "linker": linker_cpu_salted},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(8)
def test_cpu_salted_estimate_parameters_using_expectation_maximisation(
    benchmark, linker_cpu_salted
):
    cpu_count = multiprocessing.cpu_count()

    benchmark.pedantic(
        benchmark_estimate_parameters_using_expectation_maximisation,
        kwargs={"linker": linker_cpu_salted, "cpu_count": cpu_count},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(9)
def test_cpu_salted_predict(benchmark, linker_cpu_salted):
    benchmark.pedantic(
        benchmark_predict,
        kwargs={"linker": linker_cpu_salted},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(10)
def test_cpu_salted_cleanup(linker_cpu_salted):
    for k, df in linker_cpu_salted._intermediate_table_cache.items():
        if "predict" in k:
            tab = df.physical_name
            print(linker_cpu_salted.query_sql(f"select count(*) as p_count from {tab}"))
    linker_cpu_salted._con.close()
    print("closed linker_salt_2 connection")


@pytest.mark.order(11)
def test_no_salt_estimate_probability_two_random_records_match(
    benchmark, linker_no_salt
):
    benchmark.pedantic(
        benchmark_estimate_probability_two_random_records_match,
        kwargs={"linker": linker_no_salt},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(12)
def test_no_salt_estimate_u(benchmark, linker_no_salt, max_pairs):
    print(f"Max pairs = {max_pairs}")
    max_pairs = int(float(max_pairs))
    benchmark.pedantic(
        benchmark_estimate_u,
        kwargs={"max_pairs": max_pairs, "linker": linker_no_salt},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(13)
def test_no_salt_estimate_parameters_using_expectation_maximisation(
    benchmark, linker_no_salt
):
    cpu_count = multiprocessing.cpu_count()

    benchmark.pedantic(
        benchmark_estimate_parameters_using_expectation_maximisation,
        kwargs={"linker": linker_no_salt, "cpu_count": cpu_count},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(14)
def test_no_salt_predict(benchmark, linker_no_salt):
    benchmark.pedantic(
        benchmark_predict,
        kwargs={"linker": linker_no_salt},
        rounds=1,
        iterations=1,
        warmup_rounds=0,
    )


@pytest.mark.order(15)
def test_no_salt_cleanup(linker_no_salt):
    for k, df in linker_no_salt._intermediate_table_cache.items():
        if "predict" in k:
            tab = df.physical_name
            print(linker_no_salt.query_sql(f"select count(*) as p_count from {tab}"))
    linker_no_salt._con.close()
    print("closed linker_salt_2 connection")
