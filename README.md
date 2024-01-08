# test_run_benchmarks

To run benchmarks locally:

```zsh
pytest -s benchmarks/test_3m_synthetic.py --benchmark-json benchmarking_results.json --max_pairs=1e5 --num_input_rows=1e5
```
