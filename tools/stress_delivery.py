"""Multi-process concurrency stress check for the delivery API storage layer.

Spawns independent processes (each with its own Store and SQLite connection,
mirroring multiple uvicorn containers sharing one volume) that publish, claim
and acknowledge concurrently, then asserts:

* every consumer receives every committed version exactly once, in order;
* no consumer cursor ever shares state with another;
* no ack ever jumps over a pending/older version.

Run:  python tools/stress_delivery.py
"""

from __future__ import annotations

import io
import multiprocessing as mp
import sys
import tempfile
import zipfile
from pathlib import Path

# Allow running straight from the checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.store import Store, StorageError  # noqa: E402

FILES = {
    "stops.txt": "stop_id,stop_name\nS1,Central\nS2,People Square\n",
    "trips.txt": "route_id,trip_id\nR1,T1\n",
    "stop_times.txt": (
        "trip_id,arrival_time,stop_id,stop_sequence\n"
        "T1,06:00:00,S1,1\nT1,06:10:00,S2,2\n"
    ),
}


def make_feed(marker: int) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, text in FILES.items():
            zf.writestr(name, text)
        zf.writestr("marker.txt", f"v{marker}")
    return buffer.getvalue()


def publisher(data_dir: str, count: int, ready) -> None:
    ready.wait()
    store = Store(data_dir)
    for i in range(count):
        data = make_feed(i)
        import hashlib

        from app.gtfs_parse import validate_archive

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            feed = validate_archive(zf, "feed.zip")
        staging = Path(data_dir) / "staging" / f"pub-{mp.current_process().pid}-{i}.zip"
        staging.write_bytes(data)
        store.publish(
            staging_path=staging,
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            upload_filename=f"feed-{i}.zip",
            feed=feed,
        )


def consumer(data_dir: str, cid: str, expected_total: int, ready, errors) -> None:
    ready.wait()
    store = Store(data_dir)
    seen: list[int] = []
    idle = 0
    while len(seen) < expected_total and idle < 500:
        claim = store.claim_delivery(cid)
        if claim.info is None:
            idle += 1
            continue
        idle = 0
        vid = claim.info.version_id
        # Redelivery must always equal the first unacknowledged version.
        assert vid == len(seen) + 1, f"{cid}: out-of-order delivery {vid}"
        result = store.acknowledge_delivery(cid, vid, claim.info.sha256)
        assert result.advanced is True
        seen.append(vid)
        # Immediate duplicate ack: idempotent, never moves the cursor.
        dup = store.acknowledge_delivery(cid, vid, claim.info.sha256)
        assert dup.advanced is False and dup.next_version == vid + 1
    if seen != list(range(1, expected_total + 1)):
        errors.put(f"{cid}: final sequence {seen}")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(tmp)
        store.init()

        n_publishers = 3
        per_publisher = 4
        total = n_publishers * per_publisher
        consumer_ids = ["dw-alpha", "dw-beta", "dw-gamma", "dw-delta"]

        ctx = mp.get_context("spawn")
        ready = ctx.Event()
        errors = ctx.Queue()
        procs = []
        for _ in range(n_publishers):
            procs.append(
                ctx.Process(target=publisher, args=(tmp, per_publisher, ready))
            )
        for cid in consumer_ids:
            procs.append(
                ctx.Process(target=consumer, args=(tmp, cid, total, ready, errors))
            )
        for p in procs:
            p.start()
        ready.set()
        for p in procs:
            p.join(timeout=120)
            if p.exitcode != 0:
                print(f"process {p.pid} exited with {p.exitcode}")
                return 1

        queued = []
        while not errors.empty():
            queued.append(errors.get())
        if queued:
            print("ERRORS:", queued)
            return 1

        # Re-open and verify durable final state.
        check = Store(tmp)
        versions, current = check.list_versions()
        assert current == total, f"expected {total} versions, got {current}"
        states = {s.consumer_id: s for s in check.list_consumers()}
        assert set(states) == set(consumer_ids)
        for cid in consumer_ids:
            assert states[cid].next_version == total + 1, cid
            assert states[cid].last_claimed_version is None, cid

        # Every artifact still verifies after the storm.
        for info in versions:
            fd = check.open_verified_artifact(info)
            import os

            os.close(fd)

        print(
            f"OK: {total} versions, {len(consumer_ids)} consumers each received "
            f"1..{total} exactly once across {len(procs)} processes"
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
