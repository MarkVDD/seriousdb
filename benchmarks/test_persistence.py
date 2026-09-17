"""Batch persistence, serialization, and updates to existing databases."""

from pathlib import Path

import pytest

from seriousdb.cache import Cache

from ._support import (
    WARMUP_ROUNDS,
    Entries,
    assert_entries,
    verify_persisted,
    write_database,
    write_entries,
)


@pytest.mark.benchmark(group="batch-write-and-persist")
def test_batch_write_and_persist(
    benchmark,
    database_file: Path,
    entries: Entries,
    measured_rounds: int,
) -> None:
    def prepare_database():
        database_file.unlink(missing_ok=True)
        return (database_file, entries), {}

    benchmark.extra_info["persistence"] = "Cache.flush without fsync"
    benchmark.pedantic(
        write_database,
        setup=prepare_database,
        teardown=verify_persisted,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    # --benchmark-disable skips teardown; keep this explicit verification.
    verify_persisted(database_file, entries)
    benchmark.extra_info["file_bytes"] = database_file.stat().st_size


@pytest.mark.benchmark(group="seriousdb-flush")
def test_flush(
    benchmark,
    loaded_cache: Cache,
    database_file: Path,
    entries: Entries,
    measured_rounds: int,
) -> None:
    expected = dict(entries)
    changed_key = entries[0][0]

    def prepare_flush():
        # Alternate a same-sized value so every round must update the file.
        expected[changed_key] = expected[changed_key][::-1]
        loaded_cache.insert(changed_key, expected[changed_key])

    def verify():
        verify_persisted(database_file, tuple(expected.items()))

    benchmark.extra_info["persistence"] = "Cache.flush without fsync"
    benchmark.pedantic(
        loaded_cache.flush,
        setup=prepare_flush,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify()


@pytest.mark.parametrize(
    "flush_every",
    [
        pytest.param(1, id="flush-each"),
        pytest.param(100, id="flush-batch"),
    ],
)
@pytest.mark.benchmark(group="seriousdb-update-and-persist")
def test_update_and_persist(
    benchmark,
    loaded_cache: Cache,
    database_file: Path,
    entries: Entries,
    measured_rounds: int,
    flush_every: int,
) -> None:
    # A fixed operation count isolates the cost of the existing database size.
    originals = entries[:100]
    updates = tuple((key, value[::-1]) for key, value in originals)
    expected = dict(entries)
    expected.update(updates)
    expected_entries = tuple(expected.items())

    def restore_database():
        write_entries(loaded_cache, originals, len(originals))

    def verify(cache, _changed_entries, _frequency):
        assert_entries(cache, expected_entries)
        verify_persisted(database_file, expected_entries)

    benchmark.extra_info.update(
        writes=len(updates),
        flush_every=flush_every,
        persistence="Cache.flush without fsync",
    )
    benchmark.pedantic(
        write_entries,
        args=(loaded_cache, updates, flush_every),
        setup=restore_database,
        teardown=verify,
        rounds=measured_rounds,
        warmup_rounds=WARMUP_ROUNDS,
    )
    verify(loaded_cache, updates, flush_every)
