#!/bin/bash

source .venv/bin/activate
pytest benchmarks/test_splink_50k_synthetic.py  --benchmark-json benchmarking_results.json

