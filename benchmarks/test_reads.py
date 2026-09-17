"""File load and key lookup costs, measured separately and together."""

from pathlib import Path

import pytest

from seriousdb.cache import Cache

from ._support import (
    WARMUP_ROUNDS,
    Entries,
    assert_entries,
    load_and_read,
    load_cache,
    read_resident,
    write_database,
)


@pytest.mark.benchmark(group="load-and-read-by-key")
def test_load_and_read_by_key(
    benchmark,
    database_file: Path,
    entries: Entries,
    measured_rounds: int,
) -> None:
    write_database(database_file, entries)
    values: list[str] = []
    expected = [value for _, value in entries]

    def read():
        nonlocal values
        values = load_and_read(database_file, entries)

    def verify():
        assert values == expected

    benchmark.extra_info.update(
        file_bytes=database_file.stat().st_size,
        lookup="native string key",
    )
    benchmark.pedantic(
        read,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify()


@pytest.mark.benchmark(group="seriousdb-load-file")
def test_load_file(
    benchmark,
    loaded_cache: Cache,
    entries: Entries,
    measured_rounds: int,
) -> None:
    filename = loaded_cache.filename
    assert filename is not None
    cache: Cache | None = None

    def load():
        nonlocal cache
        cache = load_cache(filename)

    def verify():
        assert cache is not None
        assert_entries(cache, entries)

    benchmark.pedantic(
        load,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify()


@pytest.mark.benchmark(group="seriousdb-resident-read")
def test_resident_read(
    benchmark,
    loaded_cache: Cache,
    entries: Entries,
    measured_rounds: int,
) -> None:
    values: list[str] = []
    expected = [value for _, value in entries]

    def read():
        nonlocal values
        values = read_resident(loaded_cache, entries)

    def verify():
        assert values == expected

    benchmark.pedantic(
        read,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify()
