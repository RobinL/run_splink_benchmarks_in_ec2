# conftest.py
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--max_pairs", action="store", default="1e6", help="Maximum pairs to process"
    )


@pytest.fixture
def max_pairs(request):
    return request.config.getoption("--max_pairs")
