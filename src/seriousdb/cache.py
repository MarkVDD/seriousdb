"""In-memory key-value cache backed by a JSON file and a write-ahead log.

The whole database is held in memory as a ``dict``. Writes are durably
appended to a :class:`~seriousdb.wal.WriteAheadLog` before returning, and periodically
compacted into the full JSON snapshot file (see :data:`COMPACTION_THRESHOLD`).
All access to the data is guarded by a thread lock. File changes also hold a
sidecar lock shared by processes using the same database path.
"""

import json
import logging
import os
import tempfile
import time
from collections.abc import Iterable
from threading import Lock

from .exceptions import ResourceNotFoundError, ServiceUnavailableError
from .process_lock import locked_database
from .wal import DeleteEntry, SetEntry, WriteAheadLog, _sync_parent_directory

logger = logging.getLogger(__name__)

DEFAULT_DB = {}
COMPACTION_THRESHOLD = 50


class Cache:
    """Thread-safe in-memory key-value store persisted to a JSON file.

    A new cache holds no data. Call :meth:`load` before using it; until then
    every data access raises
    :class:`~seriousdb.exceptions.ServiceUnavailableError`.
    Writes refresh the snapshot and WAL under a process lock. Reads may retain
    an older in-memory view until this cache writes or loads again.

    Attributes
    ----------
    filename : str or None
        Path of the database file, or ``None`` if nothing has been loaded.
    wal: WriteAheadLog or None
        The write-ahead log backing this cache, or `None` if nothing has been loaded.
    db : dict of str to str or None
        The stored key-value pairs, or ``None`` if nothing has been loaded.
    lock : threading.Lock
        Lock that must be held while reading or changing `db`.
    _writes_since_compact : int
        Counter for number of writes since last compaction.
    """

    def __init__(self):
        self.filename: str | None = None
        self.wal: WriteAheadLog | None = None
        self.db: dict[str, str] | None = None
        self.lock = Lock()
        self._writes_since_compact: int = 0
        self._snapshot_identity: tuple[int, int, int, int] | None = None
        self._seen_wal_offset = 0

    def insert(self, key: str, value: str) -> tuple[str, bool]:
        """Store `value` under `key`, replacing any existing value.

        The change is appended to the write-ahead log (WAL) and must succeed there before
        it is applied in memory, so a failed write never leaves the live cache disagreeing
        with what is durable. The full database snapshot file is only rewritten periodically,
        by :meth:`_compact`.

        Parameters
        ----------
        key : str
            Key to store the value under.
        value : str
            Value to store.

        Returns
        -------
        value : str
            The stored value.
        is_new_key : bool
            ``True`` if `key` did not exist before, ``False`` if an existing
            value was replaced.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        OSError
            If the write-ahead log cannot be written. `self.db` is left unchanged in this case.
        """
        with self.lock:
            filename = require_filename(self)
            with locked_database(filename):
                db, wal, count, snapshot_identity = self._state_for_write(filename)
                is_new_key = key not in db
                wal.append(SetEntry(key=key, value=value))
                db[key] = value
                self.db, self.wal = db, wal
                self._writes_since_compact = count + 1
                self._snapshot_identity = snapshot_identity
                self._seen_wal_offset = wal._offset
                self._safe_maybe_compact()
        return value, is_new_key

    def select(self, key: str) -> str:
        """Return the value stored under `key`.

        Parameters
        ----------
        key : str
            Key to look up.

        Returns
        -------
        str
            The value stored under `key`.

        Raises
        ------
        ResourceNotFoundError
            If `key` does not exist.
        ServiceUnavailableError
            If no database has been loaded.
        """
        with self.lock:
            val = require_db(self).get(key, None)
        if val is None:
            logger.debug("Key not found: %s", key)
            raise ResourceNotFoundError(f"No value set for key {key}")
        return val

    def delete(self, key: str) -> str:
        """Remove `key` and return the value it had.

        If `key` exists, its removal is appended to the write-ahead log (WAL) and
        must succeed there before it is applied in memory, so a failed write never
        leaves the live cache disagreeing with what is durable.


        Parameters
        ----------
        key : str
            Key to remove.

        Returns
        -------
        str
            The value `key` had before it was removed.

        Raises
        ------
        ResourceNotFoundError
            If `key` does not exist.
        ServiceUnavailableError
            If no database has been loaded.
        OSError
            If the write-ahead log cannot be written. `self.db` is left unchanged in this case.
        """
        with self.lock:
            filename = require_filename(self)
            with locked_database(filename):
                db, wal, count, snapshot_identity = self._state_for_write(filename)
                val = db.get(key, None)
                if val is not None:
                    wal.append(DeleteEntry(key=key))
                    db.pop(key, None)
                    self.db, self.wal = db, wal
                    self._writes_since_compact = count + 1
                    self._snapshot_identity = snapshot_identity
                    self._seen_wal_offset = wal._offset
                    self._safe_maybe_compact()
        if val is None:
            logger.debug("Key not found: %s", key)
            raise ResourceNotFoundError(f"No value set for key {key}")
        return val

    def exists(self, key: str) -> bool:
        """Return whether `key` exists in the database.

        Parameters
        ----------
        key : str
            Key to look up.

        Returns
        -------
        bool
            ``True`` if `key` exists, ``False`` otherwise.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        with self.lock:
            return key in require_db(self)

    def __contains__(self, key: str) -> bool:
        """Return whether `key` exists in the database.

        Parameters
        ----------
        key : str
            Key to look up.

        Returns
        -------
        bool
            ``True`` if `key` exists, ``False`` otherwise.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        return self.exists(key)

    def get_all(self) -> dict[str, str]:
        """Return a snapshot of every key-value pair in the database.

        Returns
        -------
        dict of str to str
            All stored key-value pairs.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        with self.lock:
            return require_db(self).copy()

    def get_bulk(self, keys: Iterable[str]) -> dict[str, str]:
        """Return the values stored under multiple keys.

        Keys that do not exist are omitted from the result.

        Parameters
        ----------
        keys : Iterable of str
            Keys to look up.

        Returns
        -------
        dict of str to str
            A key-value pair for each requested key that exists in the database.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        key_list = tuple(keys)
        with self.lock:
            db = require_db(self)
            return {key: db[key] for key in key_list if key in db}

    def count(self) -> int:
        """Return the number of key-value pairs in the database.

        Returns
        -------
        int
            The number of stored key-value pairs.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        with self.lock:
            return len(require_db(self))

    def __len__(self) -> int:
        """Return the number of key-value pairs in the database.

        Returns
        -------
        int
            The number of stored key-value pairs.

        Raises
        ------
        ServiceUnavailableError
            If no database has been loaded.
        """
        return self.count()

    def load(self, filename: str) -> None:
        """Load the database from `filename`, replacing the current data.

        If the file does not exist, it is created with an empty database.
        If it is not valid UTF-8 JSON or does not contain a JSON object, it is
        renamed to ``<filename>.corrupt-<unix timestamp>``. If that backup already
        exists, a numeric suffix is appended (such as ``-1``, ``-2``, etc) to avoid overwriting it.
        A warning is logged, and a new file with an empty database is created in its
        place.

        After the snapshot is loaded, any entries in the write-ahead log
        (``<filename>.wal``) are replayed on top of it, recovering writes
        that happened after the last compaction.

        Parameters
        ----------
        filename : str
            Path of the database file.

        Raises
        ------
        OSError
            If the file cannot be read, renamed or written.
        """
        filename = os.path.realpath(os.path.abspath(filename))
        with self.lock, locked_database(filename):
            db, wal, count = _read_state(filename)
            self.filename = filename
            self.db, self.wal = db, wal
            self._writes_since_compact = count
            self._snapshot_identity = _snapshot_identity(filename)
            self._seen_wal_offset = wal._offset

    def flush(self) -> None:
        """No-op, kept for backward compatibility.

        Durability is now handled per-write via the write-ahead log (see
        :attr:`wal`), so nothing needs to happen here. This method
        exists so that call keeps working without change.
        """
        return

    # ------ Write-ahead log orchestration -----------------------------------------#

    def _state_for_write(
        self, filename: str
    ) -> tuple[dict[str, str], WriteAheadLog, int, tuple[int, int, int, int]]:
        """Catch up with other writers without rereading an unchanged snapshot."""
        wal = require_wal(self)
        identity = _snapshot_identity(filename)
        wal_exists = os.path.isfile(wal.filename)
        wal_size = os.path.getsize(wal.filename) if wal_exists else 0
        if (
            identity != self._snapshot_identity
            or wal_size < self._seen_wal_offset
            or (not wal_exists and self._seen_wal_offset > 0)
        ):
            db, wal, count = _read_state(filename, wal)
            return db, wal, count, _snapshot_identity(filename)

        if wal_size > self._seen_wal_offset:
            replayed = wal.replay()
            if len(replayed) < self._writes_since_compact:
                db, wal, count = _read_state(filename, wal)
                return db, wal, count, _snapshot_identity(filename)
            db = require_db(self).copy()
            for entry in replayed[self._writes_since_compact:]:
                entry.apply(db)
            return db, wal, len(replayed), identity

        return require_db(self), wal, self._writes_since_compact, identity

    def _safe_maybe_compact(self) -> None:
        """Compact if due, isolating a compaction failure from the caller.

        Called after a write has already been durably appended to the
        write-ahead log, so the write itself is safe regardless of whether
        compaction succeeds, a compaction failure must not make the
        write that triggered it look like it failed too.
        The caller holds both the thread and process locks.
        """
        try:
            if self._writes_since_compact >= COMPACTION_THRESHOLD:
                self._compact()
        except OSError as e:
            logger.error(
                "Compaction failed after durable write to %s: %s", self.filename, e
            )

    def _compact(self) -> None:
        """Write `self.db` to `self.filename` and clear the write-ahead log.

        The caller holds both the thread and process locks.

        Both the snapshot and the emptied WAL are written atomically via a temporary
        file and `os.replace`, in that order, so a crash at any point during compaction
        leaves either the old snapshot with a non-empty WAL, or the new snapshot with an
        empty WAL, and never a lost or corrupted state. Replaying the same WAL entry twice is harmless,
        since ``set``/``delete`` are overlayable.

        Raises
        ------
        OSError
            If the temporary or final files cannot be written.
        """
        if self.db is None or self.filename is None:
            return

        _atomic_write_json(self.filename, self.db)
        logger.info("Compacted database into %s", self.filename)

        if self.wal is not None:
            self.wal.clear()

        self._writes_since_compact = 0
        self._snapshot_identity = _snapshot_identity(self.filename)
        self._seen_wal_offset = 0


def _atomic_write_json(filename: str, data: dict[str, str]) -> None:
    """Write `data` to `filename` atomically, via a temp file and `os.replace`.

    Cleans up the temporary file if `os.replace` fails, rather than
    leaving it behind in the destination directory.

    Raises
    ------
    OSError
        If the temporary or final files cannot be written.
    """
    dir_name = os.path.dirname(filename) or "."
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=dir_name, delete=False) as tmp_file:
            temp_name = tmp_file.name
            tmp_file.write(json.dumps(data).encode())
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(temp_name, filename)
        _sync_parent_directory(filename)
    except Exception:
        if temp_name is not None and os.path.lexists(temp_name):
            os.unlink(temp_name)
        raise


def _write_default(filename: str) -> dict[str, str]:
    _atomic_write_json(filename, DEFAULT_DB)
    return dict(DEFAULT_DB)


def _snapshot_identity(filename: str) -> tuple[int, int, int, int]:
    """Identify the current snapshot, which compaction replaces atomically."""
    stat = os.stat(filename)
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _read_state(
    filename: str, wal: WriteAheadLog | None = None
) -> tuple[dict[str, str], WriteAheadLog, int]:
    """Read the current snapshot and WAL while the database lock is held."""
    if not os.path.isfile(filename):
        logger.info(
            "Database file %s does not exist; creating a new database", filename
        )
        db = _write_default(filename)
    else:
        try:
            with open(filename, "rb") as f:
                db = json.loads(f.read().decode())
            if not isinstance(db, dict):
                raise TypeError(f"expected dict, got {type(db).__name__}")
            logger.info("Loaded database from %s", filename)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
            backup = _generate_corrupt_backup_path(filename)
            os.replace(filename, backup)
            logger.warning(
                "Corrupt database file %s (%s); moved to %s and starting fresh",
                filename,
                e,
                backup,
            )
            db = _write_default(filename)

    if wal is None or wal.filename != f"{filename}.wal":
        wal = WriteAheadLog(f"{filename}.wal")
    replayed = wal.replay()
    for entry in replayed:
        entry.apply(db)
    return db, wal, len(replayed)


def _generate_corrupt_backup_path(filename: str) -> str:
    """Generate an unused backup path for a corrupt database file.

    The first backup uses ``<filename>.corrupt-<unix timestamp>``.
    If that path already exists, numeric suffixes such as ``-1``,
    ``-2`` and so on are tried until an unused path is found.

    Parameters
    ----------
    filename : str
        Path of the database file.

    Returns
    -------
    str
        Unused backup path.
    """
    base = f"{filename}.corrupt-{int(time.time())}"
    if not os.path.lexists(base):
        return base
    counter = 1
    while os.path.lexists(f"{base}-{counter}"):
        counter += 1
    return f"{base}-{counter}"


def require_db(cache: Cache) -> dict[str, str]:
    """Return the loaded data of `cache`.

    The caller must hold ``cache.lock`` while using the returned ``dict``.

    Parameters
    ----------
    cache : Cache
        Cache to read the data from.

    Returns
    -------
    dict of str to str
        The loaded key-value pairs. This is the cache's own ``dict``, not a
        copy.

    Raises
    ------
    ServiceUnavailableError
        If `cache` has no database loaded.
    """
    if cache.db is None:
        logger.error("Database unavailable: %s", cache.filename)
        raise ServiceUnavailableError(
            f"Database file {cache.filename} could not be opened and loaded"
        )

    return cache.db


def require_filename(cache: Cache) -> str:
    """Return the path of a loaded cache while its thread lock is held."""
    require_db(cache)
    if cache.filename is None:
        raise ServiceUnavailableError("Database file has not been loaded")
    return cache.filename


def require_wal(cache: Cache) -> WriteAheadLog:
    """Return the write-ahead log of `cache`.

    The caller must hold ``cache.lock`` while using the returned log.

    Parameters
    ----------
    cache : Cache
        Cache to read the write-ahead log from.

    Returns
    -------
    WriteAheadLog
        The cache's write-ahead log.

    Raises
    ------
    ServiceUnavailableError
        If `cache` has no database loaded.
    """
    if cache.wal is None:
        logger.error("Write-ahead log unavailable: %s", cache.filename)
        raise ServiceUnavailableError(
            f"Database file {cache.filename} could not be opened and loaded"
        )

    return cache.wal
