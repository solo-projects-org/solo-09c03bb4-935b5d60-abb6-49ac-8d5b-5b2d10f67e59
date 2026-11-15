"""Durable, atomically-published GTFS version storage.

Publication protocol (the ordering is what makes the guarantees hold):

1. The uploaded bytes are streamed to a unique staging file in ``staging/``.
2. The feed is fully validated while still in staging; invalid feeds never
   reach the database or the artifacts directory.
3. ``BEGIN IMMEDIATE`` takes SQLite's write lock, serialising concurrent
   publishers (and recovery sweeps) across threads and processes.
4. The staged ZIP is moved to a unique immutable name in ``artifacts/`` with an
   atomic rename and the directory entry is fsynced, so the bytes are durable
   before anything references them.
5. In the same transaction, the version row and all stop/trip/stop-time rows
   are inserted and committed.  SQLite commits are atomic, so a reader can
   only observe the previous version or the complete new version — never a
   half-registered one.

If step 5 fails the transaction rolls back and the freshly renamed artifact is
deleted again (and any orphan is swept at startup).  The old version was never
touched, so it stays fully queryable.  Every read request runs inside one
SQLite read transaction, giving it a stable snapshot even while a publication
is committing.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from .gtfs_parse import ParsedFeed

CURRENT_ALIASES = ("current", "latest")
READ_CHUNK = 64 * 1024
# Startup sweep only removes unreferenced artifacts older than this, which
# protects a concurrent publisher that has renamed but not yet committed.
RECOVERY_GRACE_SECONDS = 300
# Consumer identifiers are caller-provided, so pin them to a bounded safe
# charset before they ever reach a SQL parameter or a log line.
CONSUMER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class StorageError(Exception):
    """Base class for storage-layer failures mapped to HTTP responses."""

    def __init__(self, code: str, message: str, status_code: int):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class VersionNotFound(StorageError):
    def __init__(self, version: str):
        super().__init__(
            "version_not_found", f"version {version!r} does not exist", 404
        )
        self.version = version


class InvalidVersion(StorageError):
    def __init__(self, version: str):
        super().__init__(
            "invalid_version",
            f"version must be a positive integer or 'current' (got {version!r})",
            400,
        )


class NoPublishedVersion(StorageError):
    def __init__(self):
        super().__init__(
            "no_published_version", "no GTFS version has been published yet", 404
        )


class TripNotFound(StorageError):
    def __init__(self, version_id: int, trip_id: str):
        super().__init__(
            "trip_not_found",
            f"trip_id {trip_id!r} does not exist in version {version_id}",
            404,
        )


class ArtifactMissing(StorageError):
    def __init__(self, version_id: int):
        super().__init__(
            "artifact_missing",
            f"stored ZIP for version {version_id} is missing; download refused",
            410,
        )


class ArtifactCorrupt(StorageError):
    def __init__(self, version_id: int, message: str):
        super().__init__(
            "artifact_digest_mismatch",
            f"stored ZIP for version {version_id} failed integrity verification: {message}",
            409,
        )


class InvalidConsumer(StorageError):
    def __init__(self, consumer_id: str):
        super().__init__(
            "invalid_consumer_id",
            "consumer id must start with an alphanumeric character and contain "
            f"only letters, digits, '_', '.', '-' (1-128 chars); got {consumer_id!r}",
            400,
        )


class ConsumerNotFound(StorageError):
    def __init__(self, consumer_id: str):
        super().__init__(
            "consumer_not_found",
            f"consumer {consumer_id!r} has never claimed a version",
            404,
        )
        self.consumer_id = consumer_id


class AckWithoutClaim(StorageError):
    def __init__(self, consumer_id: str, version_id: int):
        super().__init__(
            "ack_without_claim",
            f"consumer {consumer_id!r} has no pending claim; version {version_id} "
            "cannot be acknowledged before it is claimed",
            409,
        )


class AckVersionMismatch(StorageError):
    def __init__(self, consumer_id: str, expected: int, requested: int):
        super().__init__(
            "ack_version_mismatch",
            f"consumer {consumer_id!r} must acknowledge version {expected} "
            f"before any other; got version {requested}",
            409,
        )


class AckDigestMismatch(StorageError):
    def __init__(self, version_id: int):
        super().__init__(
            "ack_digest_mismatch",
            f"submitted sha256 does not match the digest registered for version {version_id}",
            409,
        )


@dataclass(frozen=True)
class VersionInfo:
    version_id: int
    created_at: str
    upload_filename: str
    sha256: str
    size_bytes: int
    stops_count: int
    trips_count: int
    stop_times_count: int
    artifact_name: str


@dataclass(frozen=True)
class ConsumerState:
    consumer_id: str
    next_version: int
    last_claimed_version: Optional[int]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ClaimResult:
    """One delivery slot for a consumer.

    ``info`` is ``None`` only when every committed version has already been
    acknowledged; otherwise it is the earliest not-yet-confirmed version and
    ``replayed`` says whether the same slot was returned from a previous claim
    (a crash/redelivery rather than a fresh one).
    """

    consumer_id: str
    info: Optional[VersionInfo]
    replayed: bool
    next_version: int
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AckResult:
    consumer_id: str
    version_id: int
    advanced: bool
    next_version: int
    last_claimed_version: Optional[int]


SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    version_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT    NOT NULL,
    upload_filename TEXT    NOT NULL,
    sha256          TEXT    NOT NULL,
    size_bytes      INTEGER NOT NULL,
    artifact_name   TEXT    NOT NULL UNIQUE,
    stops_count     INTEGER NOT NULL,
    trips_count     INTEGER NOT NULL,
    stop_times_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stops (
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE,
    stop_id    TEXT    NOT NULL,
    stop_name  TEXT    NOT NULL,
    PRIMARY KEY (version_id, stop_id)
);

CREATE TABLE IF NOT EXISTS trips (
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE,
    trip_id    TEXT    NOT NULL,
    route_id   TEXT,
    PRIMARY KEY (version_id, trip_id)
);

CREATE TABLE IF NOT EXISTS stop_times (
    version_id      INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE,
    trip_id         TEXT    NOT NULL,
    stop_sequence   INTEGER NOT NULL,
    stop_id         TEXT    NOT NULL,
    arrival_time    TEXT    NOT NULL,
    arrival_seconds INTEGER NOT NULL,
    PRIMARY KEY (version_id, trip_id, stop_sequence),
    FOREIGN KEY (version_id, stop_id) REFERENCES stops(version_id, stop_id),
    FOREIGN KEY (version_id, trip_id) REFERENCES trips(version_id, trip_id)
);

-- Per-consumer delivery cursor.  No foreign key on next_version / last_claimed
-- on purpose: versions are append-only and never deleted, and keeping this
-- table independent means delivery state always survives schema changes.
-- next_version       : earliest version the consumer has not acknowledged.
-- last_claimed_version: version handed out by the latest claim while it is
--                       still unacknowledged; NULL once it is confirmed, which
--                       makes a redelivery after a crash repeat the same row.
CREATE TABLE IF NOT EXISTS consumer_progress (
    consumer_id          TEXT    PRIMARY KEY,
    next_version         INTEGER NOT NULL CHECK (next_version >= 1),
    last_claimed_version INTEGER REFERENCES versions(version_id),
    created_at           TEXT    NOT NULL,
    updated_at           TEXT    NOT NULL,
    CHECK (last_claimed_version IS NULL OR last_claimed_version = next_version)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class Store:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.artifacts_dir = self.data_dir / "artifacts"
        self.staging_dir = self.data_dir / "staging"
        self.db_path = self.data_dir / "gtfs.db"
        # Serialises publications inside this process; SQLite's own
        # BEGIN IMMEDIATE serialises them across processes as well.
        self._publish_lock = threading.Lock()

    # ------------------------------------------------------------------ setup
    def init(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        self._recover()

    def _recover(self) -> None:
        """Remove interrupted uploads and unreferenced artifacts.

        Runs at startup before requests are served.  Registered artifacts are
        never deleted, so committed versions keep working across restarts.

        The sweep holds SQLite's write lock, so it cannot race with another
        process that is between its artifact rename and its commit; the
        publication path takes the same lock before renaming, and only young
        files are swept as an extra safeguard.
        """
        for entry in self.staging_dir.iterdir():
            with contextlib.suppress(OSError):
                if entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    entry.unlink()

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute("SELECT artifact_name FROM versions").fetchall()
            referenced = {row["artifact_name"] for row in rows}
            cutoff = time.time() - RECOVERY_GRACE_SECONDS
            for entry in self.artifacts_dir.iterdir():
                if entry.name in referenced or not entry.is_file():
                    continue
                with contextlib.suppress(OSError):
                    if entry.stat().st_mtime < cutoff:
                        entry.unlink()
            conn.execute("COMMIT")
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            conn.close()

    # ------------------------------------------------------------- connections
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,  # explicit BEGIN/COMMIT control
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextlib.contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        """One consistent snapshot for the whole block."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("COMMIT")
            conn.close()

    # ------------------------------------------------------------ publication
    def publish(
        self,
        staging_path: Path,
        sha256: str,
        size_bytes: int,
        upload_filename: str,
        feed: ParsedFeed,
    ) -> Tuple[VersionInfo, Optional[int]]:
        """Atomically publish a validated feed.

        Returns the new version plus the version that was current immediately
        before this publication (read inside the same write transaction, so it
        is exact even under concurrent uploads).
        """
        artifact_name = f"{uuid.uuid4().hex}.zip"
        artifact_path = self.artifacts_dir / artifact_name
        created_at = utc_now()
        moved = False
        with self._publish_lock:
            conn = self._connect()
            try:
                # Take the write lock first: while it is held, no recovery sweep
                # in another process can mistake the freshly renamed artifact
                # for an orphan, and concurrent publishers are serialised.
                conn.execute("BEGIN IMMEDIATE")
                current_row = conn.execute(
                    "SELECT MAX(version_id) AS current FROM versions"
                ).fetchone()
                previous = (
                    int(current_row["current"])
                    if current_row is not None and current_row["current"] is not None
                    else None
                )
                # Durable bytes before registration.
                os.replace(staging_path, artifact_path)
                moved = True
                self._fsync_dir(self.artifacts_dir)
                cursor = conn.execute(
                    "INSERT INTO versions (created_at, upload_filename, sha256, "
                    "size_bytes, artifact_name, stops_count, trips_count, "
                    "stop_times_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        created_at,
                        upload_filename,
                        sha256,
                        size_bytes,
                        artifact_name,
                        len(feed.stops),
                        len(feed.trips),
                        feed.total_stop_times,
                    ),
                )
                version_id = int(cursor.lastrowid)
                self._insert_rows(conn, version_id, feed)
                conn.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                # The version row was never committed, so this file is not
                # referenced by anything.  Removing it keeps the data dir clean
                # (and _recover() would sweep it after a crash anyway).
                if moved:
                    with contextlib.suppress(OSError):
                        artifact_path.unlink()
                raise
            finally:
                conn.close()

            return (
                VersionInfo(
                    version_id=version_id,
                    created_at=created_at,
                    upload_filename=upload_filename,
                    sha256=sha256,
                    size_bytes=size_bytes,
                    stops_count=len(feed.stops),
                    trips_count=len(feed.trips),
                    stop_times_count=feed.total_stop_times,
                    artifact_name=artifact_name,
                ),
                previous,
            )

    def _insert_rows(
        self, conn: sqlite3.Connection, version_id: int, feed: ParsedFeed
    ) -> None:
        conn.executemany(
            "INSERT INTO stops (version_id, stop_id, stop_name) VALUES (?, ?, ?)",
            [
                (version_id, stop.stop_id, stop.stop_name)
                for stop in feed.stops.values()
            ],
        )
        conn.executemany(
            "INSERT INTO trips (version_id, trip_id, route_id) VALUES (?, ?, ?)",
            [
                (version_id, trip.trip_id, trip.route_id)
                for trip in feed.trips.values()
            ],
        )
        rows: List[Tuple[int, str, int, str, str, int]] = []
        for trip_id, stop_times in feed.stop_times_by_trip.items():
            for stop_time in stop_times:
                rows.append(
                    (
                        version_id,
                        trip_id,
                        stop_time.stop_sequence,
                        stop_time.stop_id,
                        stop_time.arrival_time,
                        stop_time.arrival_seconds,
                    )
                )
        conn.executemany(
            "INSERT INTO stop_times (version_id, trip_id, stop_sequence, stop_id, "
            "arrival_time, arrival_seconds) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        with contextlib.suppress(OSError):
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    # -------------------------------------------------------------- delivery
    @staticmethod
    def _validate_consumer_id(consumer_id: str) -> str:
        value = (consumer_id or "").strip()
        if not CONSUMER_ID_RE.match(value):
            raise InvalidConsumer(consumer_id)
        return value

    @staticmethod
    def _consumer_state(row: sqlite3.Row) -> ConsumerState:
        return ConsumerState(
            consumer_id=row["consumer_id"],
            next_version=int(row["next_version"]),
            last_claimed_version=(
                int(row["last_claimed_version"])
                if row["last_claimed_version"] is not None
                else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_consumer(self, consumer_id: str) -> ConsumerState:
        consumer_id = self._validate_consumer_id(consumer_id)
        with self.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM consumer_progress WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()
            if row is None:
                raise ConsumerNotFound(consumer_id)
            return self._consumer_state(row)

    def list_consumers(self) -> List[ConsumerState]:
        with self.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM consumer_progress ORDER BY consumer_id"
            ).fetchall()
        return [self._consumer_state(row) for row in rows]

    def claim_delivery(self, consumer_id: str) -> ClaimResult:
        """Hand the consumer its earliest unacknowledged committed version.

        Runs in a single ``BEGIN IMMEDIATE`` transaction so concurrent claims
        from several processes serialise on SQLite's write lock; each consumer
        row is only ever touched by its own transactions, and the version row
        is read from the same committed snapshot as the cursor, so a claim can
        never observe a half-published version or jump over an older one.

        A consumer whose first claim arrives after versions 1..N exist starts
        at version 1 (backfill).  Repeating a claim while the previous slot is
        unacknowledged returns the same version, which is what makes the
        protocol restart-safe.
        """
        consumer_id = self._validate_consumer_id(consumer_id)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = utc_now()
            row = conn.execute(
                "SELECT * FROM consumer_progress WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()
            if row is None:
                # First contact: backfill starts from the oldest version.
                conn.execute(
                    "INSERT INTO consumer_progress (consumer_id, next_version, "
                    "last_claimed_version, created_at, updated_at) "
                    "VALUES (?, 1, NULL, ?, ?)",
                    (consumer_id, now, now),
                )
                next_version = 1
                last_claimed = None
                created_at = now
            else:
                next_version = int(row["next_version"])
                last_claimed = (
                    int(row["last_claimed_version"])
                    if row["last_claimed_version"] is not None
                    else None
                )
                created_at = row["created_at"]

            if last_claimed is not None:
                # Pending slot: redeliver exactly the same version.  Nothing is
                # mutated, so the claim stays idempotent across crashes.
                target = last_claimed
                replayed = True
            else:
                target = next_version
                replayed = False

            version_row = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?", (target,)
            ).fetchone()
            if version_row is None:
                # Nothing committed at the frontier (yet).  Leave the cursor
                # untouched; a later claim after a new publication picks it up.
                result = ClaimResult(
                    consumer_id=consumer_id,
                    info=None,
                    replayed=replayed,
                    next_version=next_version,
                    created_at=created_at,
                    updated_at=now,
                )
                conn.execute("COMMIT")
                return result

            if not replayed:
                conn.execute(
                    "UPDATE consumer_progress SET last_claimed_version = ?, "
                    "updated_at = ? WHERE consumer_id = ?",
                    (target, now, consumer_id),
                )
            conn.execute("COMMIT")
            return ClaimResult(
                consumer_id=consumer_id,
                info=self._version_info(version_row),
                replayed=replayed,
                next_version=next_version,
                created_at=created_at,
                updated_at=now,
            )
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def acknowledge_delivery(
        self, consumer_id: str, version_id: int, sha256: str
    ) -> AckResult:
        """Advance the cursor only after a verified, correctly-sequenced ack.

        Three cases, decided inside one ``BEGIN IMMEDIATE`` transaction:

        * The version is the consumer's currently pending claim.  The supplied
          digest must equal the registered digest and the artifact on disk must
          exist and hash to it; only then does ``next_version`` move forward and
          the pending claim clear (``advanced=True``).
        * The version was already confirmed earlier (a retried/lost-response
          ack).  The registered digest must still match; the call succeeds
          idempotently (``advanced=False``) and never moves the cursor, so a
          duplicate can never skip the next version.
        * Any other version is rejected: future versions would skip the
          backlog, and an unclaimed frontier version must be claimed first.

        ArtifactMissing (410) / ArtifactCorrupt (409) from on-disk verification
        propagate to the API; after the artifact is repaired the same claim and
        ack can be retried because the cursor has not moved.
        """
        consumer_id = self._validate_consumer_id(consumer_id)
        if not isinstance(version_id, int) or version_id <= 0:
            raise InvalidVersion(str(version_id))
        digest = (sha256 or "").strip().lower()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            consumer = conn.execute(
                "SELECT * FROM consumer_progress WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()
            if consumer is None:
                raise ConsumerNotFound(consumer_id)

            next_version = int(consumer["next_version"])
            last_claimed = (
                int(consumer["last_claimed_version"])
                if consumer["last_claimed_version"] is not None
                else None
            )

            version_row = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if version_row is None:
                raise VersionNotFound(str(version_id))
            if digest != version_row["sha256"]:
                raise AckDigestMismatch(version_id)

            if last_claimed is not None and version_id == last_claimed:
                # Normal path: confirm the pending claim.  Verify the actual
                # artifact before moving the cursor; both happen in this one
                # transaction, so a crash simply repeats the attempt.
                info = self._version_info(version_row)
                fd = self.open_verified_artifact(info)
                os.close(fd)

                now = utc_now()
                conn.execute(
                    "UPDATE consumer_progress SET next_version = ?, "
                    "last_claimed_version = NULL, updated_at = ? "
                    "WHERE consumer_id = ?",
                    (version_id + 1, now, consumer_id),
                )
                conn.execute("COMMIT")
                return AckResult(
                    consumer_id=consumer_id,
                    version_id=version_id,
                    advanced=True,
                    next_version=version_id + 1,
                    last_claimed_version=None,
                )

            if version_id < next_version:
                # Idempotent retry of an already-acknowledged version.  The
                # cursor is untouched (and therefore cannot skip anything);
                # only the registered digest is re-checked above.
                conn.execute("COMMIT")
                return AckResult(
                    consumer_id=consumer_id,
                    version_id=version_id,
                    advanced=False,
                    next_version=next_version,
                    last_claimed_version=last_claimed,
                )

            # version_id >= next_version without a matching pending claim:
            # either skipping ahead, or acknowledging something never claimed.
            if last_claimed is not None:
                raise AckVersionMismatch(consumer_id, last_claimed, version_id)
            raise AckWithoutClaim(consumer_id, version_id)
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------ resolution
    def _resolve_version(self, conn: sqlite3.Connection, version: str) -> int:
        value = (version or "").strip()
        if value.lower() in CURRENT_ALIASES:
            row = conn.execute(
                "SELECT MAX(version_id) AS current FROM versions"
            ).fetchone()
            if row is None or row["current"] is None:
                raise NoPublishedVersion()
            return int(row["current"])
        if not value.isdigit() or int(value) <= 0:
            raise InvalidVersion(version)
        version_id = int(value)
        row = conn.execute(
            "SELECT 1 FROM versions WHERE version_id = ?", (version_id,)
        ).fetchone()
        if row is None:
            raise VersionNotFound(version)
        return version_id

    def resolve_version(self, version: str) -> int:
        with self.read_transaction() as conn:
            return self._resolve_version(conn, version)

    # ---------------------------------------------------------------- queries
    @staticmethod
    def _version_info(row: sqlite3.Row) -> VersionInfo:
        return VersionInfo(
            version_id=row["version_id"],
            created_at=row["created_at"],
            upload_filename=row["upload_filename"],
            sha256=row["sha256"],
            size_bytes=row["size_bytes"],
            stops_count=row["stops_count"],
            trips_count=row["trips_count"],
            stop_times_count=row["stop_times_count"],
            artifact_name=row["artifact_name"],
        )

    def get_version(self, version: str) -> Tuple[VersionInfo, int]:
        """Return ``(metadata, current_version_id)`` from one snapshot."""
        with self.read_transaction() as conn:
            version_id = self._resolve_version(conn, version)
            row = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if row is None:  # pragma: no cover - resolved above
                raise VersionNotFound(version)
            current = conn.execute(
                "SELECT MAX(version_id) AS current FROM versions"
            ).fetchone()["current"]
        return self._version_info(row), int(current)

    def list_versions(self) -> Tuple[List[VersionInfo], Optional[int]]:
        with self.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM versions ORDER BY version_id DESC"
            ).fetchall()
            current = conn.execute(
                "SELECT MAX(version_id) AS current FROM versions"
            ).fetchone()["current"]
        return [self._version_info(row) for row in rows], (
            int(current) if current is not None else None
        )

    def trip_stops(
        self, version: str, trip_id: str
    ) -> Tuple[VersionInfo, Optional[str], List[dict]]:
        """Version metadata + ordered stop list, all from one snapshot."""
        with self.read_transaction() as conn:
            version_id = self._resolve_version(conn, version)
            version_row = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if version_row is None:  # pragma: no cover - resolved above
                raise VersionNotFound(version)
            trip_row = conn.execute(
                "SELECT trip_id, route_id FROM trips "
                "WHERE version_id = ? AND trip_id = ?",
                (version_id, trip_id),
            ).fetchone()
            if trip_row is None:
                raise TripNotFound(version_id, trip_id)
            rows = conn.execute(
                "SELECT st.stop_sequence, st.stop_id, s.stop_name, "
                "       st.arrival_time, st.arrival_seconds "
                "FROM stop_times AS st "
                "JOIN stops AS s "
                "  ON s.version_id = st.version_id AND s.stop_id = st.stop_id "
                "WHERE st.version_id = ? AND st.trip_id = ? "
                "ORDER BY st.stop_sequence",
                (version_id, trip_id),
            ).fetchall()
        stops = [
            {
                "stop_sequence": row["stop_sequence"],
                "stop_id": row["stop_id"],
                "stop_name": row["stop_name"],
                "arrival_time": row["arrival_time"],
                "arrival_seconds": row["arrival_seconds"],
            }
            for row in rows
        ]
        return self._version_info(version_row), trip_row["route_id"], stops

    # ------------------------------------------------------------- artifacts
    def artifact_path(self, info: VersionInfo) -> Path:
        name = info.artifact_name
        if not name or Path(name).name != name or "/" in name or "\\" in name:
            raise ArtifactCorrupt(info.version_id, "invalid artifact name in registry")
        return self.artifacts_dir / name

    def open_verified_artifact(self, info: VersionInfo) -> int:
        """Open and fully verify the ZIP; return a read handle positioned at 0.

        Verification and response streaming share the same file descriptor, so
        swapping the file on disk cannot make a served body differ from the one
        that was verified.
        """
        path = self.artifact_path(info)
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            raise ArtifactMissing(info.version_id)
        except OSError as exc:
            raise ArtifactCorrupt(info.version_id, f"cannot open artifact: {exc.errno}")
        try:
            digest = hashlib.sha256()
            remaining = info.size_bytes
            while True:
                chunk = os.read(fd, READ_CHUNK)
                if not chunk:
                    break
                remaining -= len(chunk)
                if remaining < 0:
                    raise ArtifactCorrupt(
                        info.version_id,
                        f"size exceeds registered {info.size_bytes} bytes",
                    )
                digest.update(chunk)
            if remaining != 0:
                raise ArtifactCorrupt(
                    info.version_id,
                    f"size is {info.size_bytes - remaining} bytes but "
                    f"{info.size_bytes} bytes were registered",
                )
            if digest.hexdigest() != info.sha256:
                raise ArtifactCorrupt(
                    info.version_id,
                    "sha256 does not match the published digest",
                )
            os.lseek(fd, 0, os.SEEK_SET)
        except BaseException:
            os.close(fd)
            raise
        return fd


def conn_row_time(conn: sqlite3.Connection, version_id: int) -> str:  # pragma: no cover
    """Unused compatibility shim kept out of the hot path."""
    raise NotImplementedError


def iter_fd(fd: int, size: int) -> Iterator[bytes]:
    """Yield the verified artifact from its open descriptor, then close it."""
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        remaining = size
        while remaining > 0:
            chunk = os.read(fd, min(READ_CHUNK, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk
    finally:
        os.close(fd)
