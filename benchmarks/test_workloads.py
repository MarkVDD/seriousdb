"""A deterministic read-heavy workload on one loaded cache."""

from pathlib import Path

import pytest

from seriousdb.cache import Cache

from ._support import (
    WARMUP_ROUNDS,
    Entries,
    assert_entries,
    expected_state,
    mixed_operations,
    run_operations,
    verify_persisted,
)


@pytest.mark.benchmark(group="seriousdb-mixed-resident")
def test_mixed_resident(
    benchmark,
    loaded_cache: Cache,
    database_file: Path,
    entries: Entries,
    measured_rounds: int,
) -> None:
    operations = mixed_operations(entries)
    expected = expected_state(entries, operations)
    expected_reads = [value for action, _, value in operations if action == "read"]
    values: list[str] = []

    def run():
        nonlocal values
        values = run_operations(loaded_cache, operations)

    def restore_entries():
        for key, value in entries:
            loaded_cache.insert(key, value)

    def verify():
        assert values == expected_reads
        assert_entries(loaded_cache, expected)

    benchmark.extra_info.update(
        reads=len(expected_reads),
        writes=len(entries) - len(expected_reads),
        persistence="in-memory only",
    )
    benchmark.pedantic(
        run,
        setup=restore_entries,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify()
    loaded_cache.flush()
    verify_persisted(database_file, expected)
