"""Benchmark-only options, dataset selection, and isolated database fixtures."""

from argparse import ArgumentTypeError
from importlib.metadata import version
from pathlib import Path

import pytest

from ._support import (
    RANDOM_SEED,
    WORKLOAD_VERSION,
    Entries,
    load_cache,
    make_entries,
    write_database,
)

DEFAULT_DATASETS = [(100, 32), (1_000, 32), (1_000, 1_024)]
EXTENDED_DATASETS = [(10_000, 32), (100_000, 32)]


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise ArgumentTypeError("must be a positive integer")
    return number


def pytest_addoption(parser):
    group = parser.getgroup("seriousdb benchmarks")
    group.addoption(
        "--extended", action="store_true", help="Add 10k/100k SeriousDB datasets."
    )
    group.addoption(
        "--engine-rounds",
        type=_positive_int,
        default=5,
        help="Measured rounds per scenario (default: 5).",
    )


def pytest_generate_tests(metafunc):
    if "entries" not in metafunc.fixturenames:
        return
    datasets = DEFAULT_DATASETS.copy()
    if metafunc.config.getoption("--extended"):
        datasets.extend(EXTENDED_DATASETS)

    metafunc.parametrize(
        "entries",
        [
            pytest.param(dataset, id=f"{dataset[0]}x{dataset[1]}B")
            for dataset in datasets
        ],
        indirect=True,
    )


@pytest.fixture
def measured_rounds(request) -> int:
    return request.config.getoption("--engine-rounds")


@pytest.fixture
def entries(request, benchmark) -> Entries:
    count, value_bytes = request.param
    benchmark.extra_info.update(
        entries=count,
        value_bytes=value_bytes,
        seed=RANDOM_SEED,
        seriousdb_version=version("seriousdb"),
        workload_version=WORKLOAD_VERSION,
        persistence="none during timed work",
        workers=1,
    )
    return make_entries(count, value_bytes)


@pytest.fixture
def database_file(tmp_path: Path) -> Path:
    return tmp_path / "database.json"


@pytest.fixture
def loaded_cache(database_file: Path, entries: Entries):
    write_database(database_file, entries)
    return load_cache(str(database_file))
