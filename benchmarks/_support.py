"""Shared data generation and cache operations for benchmark scenarios."""

from pathlib import Path
from random import Random

from seriousdb.cache import Cache

Entries = tuple[tuple[str, str], ...]
Operations = list[tuple[str, str, str]]
RANDOM_SEED = 212
WARMUP_ROUNDS = 1
WORKLOAD_VERSION = 5


def make_entries(count: int, value_bytes: int) -> Entries:
    entries = [
        (f"key-{i:06}", f"value-{i:06}".ljust(value_bytes, "x")) for i in range(count)
    ]
    Random(RANDOM_SEED).shuffle(entries)
    return tuple(entries)


def load_cache(filename: str) -> Cache:
    cache = Cache()
    cache.load(filename)
    return cache


def read_resident(cache: Cache, entries: Entries) -> list[str]:
    return [cache.select(key) for key, _ in entries]


def assert_entries(cache: Cache, entries: Entries) -> None:
    assert read_resident(cache, entries) == [value for _, value in entries]
    with cache.lock:
        assert cache.db is not None
        assert len(cache.db) == len(entries)


def write_entries(cache: Cache, entries: Entries, flush_every: int) -> None:
    if flush_every < 1:
        raise ValueError("flush_every must be positive")
    for count, (key, value) in enumerate(entries, start=1):
        cache.insert(key, value)
        if count % flush_every == 0:
            cache.flush()
    if len(entries) % flush_every:
        cache.flush()


def write_database(database_file: Path, entries: Entries) -> None:
    cache = load_cache(str(database_file))
    write_entries(cache, entries, len(entries))


def load_and_read(database_file: Path, entries: Entries) -> list[str]:
    return read_resident(load_cache(str(database_file)), entries)


def verify_persisted(database_file: Path, entries: Entries) -> None:
    assert_entries(load_cache(str(database_file)), entries)


def mixed_operations(entries: Entries) -> Operations:
    operations = [("read", key, value) for key, value in entries]
    write_count = len(entries) // 10
    # Overwrites replace reads for 10% of keys, keeping reads deterministic.
    operations[:write_count] = [
        ("write", key, value[::-1]) for key, value in entries[:write_count]
    ]
    Random(RANDOM_SEED).shuffle(operations)
    return operations


def run_operations(cache: Cache, operations: Operations) -> list[str]:
    values = []
    for action, key, value in operations:
        if action == "read":
            values.append(cache.select(key))
        else:
            cache.insert(key, value)
    return values


def expected_state(entries: Entries, operations: Operations) -> Entries:
    expected = dict(entries)
    expected.update(
        (key, value) for action, key, value in operations if action == "write"
    )
    return tuple(expected.items())
