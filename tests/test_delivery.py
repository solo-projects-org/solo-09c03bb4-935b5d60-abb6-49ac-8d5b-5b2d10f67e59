"""Tests for the incremental per-consumer delivery API.

Covers the delivery contract:

* first claim backfills already-published versions in publication order;
* an unacknowledged claim is redelivered (same version, replayed=true), also
  across a process restart, so consumers resume after a crash;
* acknowledgement advances only when version id *and* digest match, and only
  while the on-disk artifact verifies;
* duplicate acks are idempotent and never skip a version;
* missing/corrupt artifacts block acknowledgement but remain retryable after
  repair;
* consumers keep independent cursors;
* concurrent publishes, claims and acks never deliver a half version, jump
  over an older one, or share progress between consumers.
"""

from __future__ import annotations

import hashlib
import threading
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.store import Store

from test_api import BASE_FILES, upload, zip_bytes


# ---------------------------------------------------------------------------
# Fixtures / small helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "data"
    path.mkdir()
    return path


@pytest.fixture()
def client(data_dir: Path):
    store = Store(data_dir)
    store.init()
    main_module.set_store(store)
    with TestClient(main_module.app) as test_client:
        yield test_client
    main_module._store = None


CLAIM = "/api/v1/delivery/consumers/{cid}/claim"
ACK = "/api/v1/delivery/consumers/{cid}/ack"
PROGRESS = "/api/v1/delivery/consumers/{cid}"


def claim(client: TestClient, cid: str):
    return client.post(CLAIM.format(cid=cid))


def ack(client: TestClient, cid: str, version_id: int, sha256: str):
    return client.post(
        ACK.format(cid=cid),
        json={"version_id": version_id, "sha256": sha256},
    )


def publish_two(client: TestClient):
    first = upload(client).json()["version"]
    second = upload(client).json()["version"]
    return first, second


# ---------------------------------------------------------------------------
# Claim / backfill / order
# ---------------------------------------------------------------------------


def test_first_claim_backfills_oldest_committed_version(client: TestClient):
    first, second = publish_two(client)

    # Consumer appears only after versions exist: it must start at version 1.
    response = claim(client, "dw-a")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["consumer_id"] == "dw-a"
    assert body["replayed"] is False
    assert body["next_version"] == 1
    assert body["last_claimed_version"] == 1
    assert body["version"]["version_id"] == 1
    assert body["version"]["sha256"] == first["sha256"]
    assert body["download_url"] == "/api/v1/versions/1/download"


def test_versions_are_delivered_in_publication_order(client: TestClient):
    first, second = publish_two(client)

    c1 = claim(client, "dw-a").json()
    assert c1["version"]["version_id"] == 1

    assert (
        ack(client, "dw-a", 1, first["sha256"]).json()["next_version"] == 2
    )

    c2 = claim(client, "dw-a").json()
    assert c2["replayed"] is False
    assert c2["next_version"] == 2
    assert c2["last_claimed_version"] == 2
    assert c2["version"]["version_id"] == 2
    assert c2["version"]["sha256"] == second["sha256"]
    assert c2["download_url"] == "/api/v1/versions/2/download"

    assert ack(client, "dw-a", 2, second["sha256"]).status_code == 200


def test_unacknowledged_claim_is_redelivered_same_version(client: TestClient):
    first, _ = publish_two(client)

    c1 = claim(client, "dw-a").json()
    assert c1["version"]["version_id"] == 1
    assert c1["replayed"] is False

    # Repeated claims while version 1 is not confirmed must keep returning 1.
    for _ in range(3):
        again = claim(client, "dw-a").json()
        assert again["version"]["version_id"] == 1
        assert again["replayed"] is True
        assert again["next_version"] == 1
        assert again["last_claimed_version"] == 1
        assert again["version"]["sha256"] == first["sha256"]
        assert again["download_url"] == "/api/v1/versions/1/download"

    progress = client.get(PROGRESS.format(cid="dw-a")).json()
    assert progress["next_version"] == 1
    assert progress["last_claimed_version"] == 1


def test_claim_returns_null_when_everything_acknowledged(client: TestClient):
    first, second = publish_two(client)
    # Version 1: claim -> ack.
    claim(client, "dw-a")
    ack(client, "dw-a", 1, first["sha256"])
    # Version 2: claim -> ack.
    claim(client, "dw-a")
    ack(client, "dw-a", 2, second["sha256"])

    # The ack cleared the pending slot; the next claim finds nothing to deliver.
    response = claim(client, "dw-a")
    assert response.status_code == 200
    body = response.json()
    assert body["version"] is None
    assert body["download_url"] is None
    assert body["last_claimed_version"] is None
    assert body["next_version"] == 3
    assert body["replayed"] is False

    # A newly published version becomes deliverable on the next claim.
    third = upload(client).json()["version"]
    delivered = claim(client, "dw-a").json()
    assert delivered["version"]["version_id"] == 3
    assert delivered["version"]["sha256"] == third["sha256"]
    assert delivered["replayed"] is False


def test_claim_before_any_publication_returns_null(client: TestClient):
    body = claim(client, "early-bird").json()
    assert body["version"] is None
    assert body["next_version"] == 1
    assert body["last_claimed_version"] is None

    published = upload(client).json()["version"]
    delivered = claim(client, "early-bird").json()
    assert delivered["version"]["version_id"] == 1
    assert delivered["version"]["sha256"] == published["sha256"]


# ---------------------------------------------------------------------------
# Acknowledgement rules
# ---------------------------------------------------------------------------


def test_ack_advances_only_with_matching_version_and_digest(client: TestClient):
    first, _ = publish_two(client)
    claim(client, "dw-a")

    good = ack(client, "dw-a", 1, first["sha256"])
    assert good.status_code == 200
    body = good.json()
    assert body["advanced"] is True
    assert body["next_version"] == 2
    assert body["last_claimed_version"] is None

    progress = client.get(PROGRESS.format(cid="dw-a")).json()
    assert progress["next_version"] == 2
    assert progress["last_claimed_version"] is None


def test_ack_with_wrong_digest_is_rejected_and_cursor_holds(client: TestClient):
    first, _ = publish_two(client)
    claim(client, "dw-a")

    response = ack(client, "dw-a", 1, "deadbeef" * 8)
    assert response.status_code == 409
    assert response.json()["error"] == "ack_digest_mismatch"

    # Cursor unchanged: the same version is still redelivered.
    redelivered = claim(client, "dw-a").json()
    assert redelivered["version"]["version_id"] == 1
    assert redelivered["replayed"] is True


def test_ack_cannot_skip_to_a_future_version(client: TestClient):
    first, second = publish_two(client)
    claim(client, "dw-a")  # pending version 1

    response = ack(client, "dw-a", 2, second["sha256"])
    assert response.status_code == 409
    assert response.json()["error"] == "ack_version_mismatch"

    # Version 1 is still the pending delivery.
    assert claim(client, "dw-a").json()["version"]["version_id"] == 1
    # And the rejected ack did not confirm anything.
    assert ack(client, "dw-a", 1, first["sha256"]).status_code == 200


def test_ack_before_any_claim_is_rejected(client: TestClient):
    first, second = publish_two(client)
    response = ack(client, "dw-new", 1, first["sha256"])
    assert response.status_code == 404
    assert response.json()["error"] == "consumer_not_found"

    # A consumer that only drained (no pending slot) cannot ack the frontier.
    claim(client, "dw-b")
    ack(client, "dw-b", 1, first["sha256"])
    # Use the correct digest so the request reaches the sequencing check.
    response = ack(client, "dw-b", 2, second["sha256"])
    assert response.status_code == 409
    assert response.json()["error"] == "ack_without_claim"


def test_duplicate_ack_is_idempotent_and_never_skips(client: TestClient):
    first, second = publish_two(client)
    claim(client, "dw-a")
    assert ack(client, "dw-a", 1, first["sha256"]).json()["advanced"] is True

    # Retried/lost-response ack of the same version: accepted, no movement.
    duplicate = ack(client, "dw-a", 1, first["sha256"])
    assert duplicate.status_code == 200
    body = duplicate.json()
    assert body["advanced"] is False
    assert body["next_version"] == 2

    # The next version still has to be claimed and confirmed normally.
    assert claim(client, "dw-a").json()["version"]["version_id"] == 2
    second_ack = ack(client, "dw-a", 2, second["sha256"])
    assert second_ack.status_code == 200
    assert second_ack.json()["advanced"] is True
    assert second_ack.json()["next_version"] == 3


def test_duplicate_ack_still_checks_the_registered_digest(client: TestClient):
    first, _ = publish_two(client)
    claim(client, "dw-a")
    ack(client, "dw-a", 1, first["sha256"])

    response = ack(client, "dw-a", 1, "a" * 64)
    assert response.status_code == 409
    assert response.json()["error"] == "ack_digest_mismatch"


# ---------------------------------------------------------------------------
# Artifact integrity gates the ack
# ---------------------------------------------------------------------------


def test_missing_artifact_blocks_ack_then_repairs_and_continues(
    client: TestClient,
):
    first, second = publish_two(client)
    claim(client, "dw-a")

    store = main_module.get_store()
    info, _ = store.get_version("1")
    artifact = store.artifact_path(info)
    artifact.unlink()

    response = ack(client, "dw-a", 1, first["sha256"])
    assert response.status_code == 410
    assert response.json()["error"] == "artifact_missing"
    assert response.headers["cache-control"] == "no-store"

    # Nothing advanced: the claim is still replayable.
    assert claim(client, "dw-a").json()["version"]["version_id"] == 1

    # Restore the original bytes and the same ack now succeeds.
    artifact.write_bytes(zip_bytes())
    recovered = ack(client, "dw-a", 1, first["sha256"])
    assert recovered.status_code == 200
    assert recovered.json()["next_version"] == 2
    assert claim(client, "dw-a").json()["version"]["version_id"] == 2


def test_corrupt_artifact_blocks_ack_until_repaired(client: TestClient):
    first, _ = publish_two(client)
    claim(client, "dw-a")

    store = main_module.get_store()
    info, _ = store.get_version("1")
    artifact = store.artifact_path(info)
    artifact.write_bytes(artifact.read_bytes() + b"tampered")

    response = ack(client, "dw-a", 1, first["sha256"])
    assert response.status_code == 409
    assert response.json()["error"] == "artifact_digest_mismatch"

    # The claim still hands out the same slot after the failed ack.
    assert claim(client, "dw-a").json()["replayed"] is True

    artifact.write_bytes(zip_bytes())
    assert ack(client, "dw-a", 1, first["sha256"]).status_code == 200


def test_download_url_serves_the_claimed_original_bytes(client: TestClient):
    data = zip_bytes()
    published = upload(client, data).json()["version"]
    delivered = claim(client, "dw-a").json()

    download = client.get(delivered["download_url"])
    assert download.status_code == 200
    assert download.content == data
    assert hashlib.sha256(download.content).hexdigest() == published["sha256"]


# ---------------------------------------------------------------------------
# Consumer isolation, validation and observability
# ---------------------------------------------------------------------------


def test_consumers_keep_independent_progress(client: TestClient):
    first, second = publish_two(client)

    # Consumer A fully confirms version 1.
    claim(client, "dw-a")
    ack(client, "dw-a", 1, first["sha256"])

    # Consumer B starts afterwards and must still backfill version 1.
    b_first = claim(client, "dw-b").json()
    assert b_first["next_version"] == 1
    assert b_first["version"]["version_id"] == 1
    assert b_first["replayed"] is False

    # A's pending work is version 2 and B's is version 1.
    a_next = claim(client, "dw-a").json()
    assert a_next["version"]["version_id"] == 2
    assert claim(client, "dw-b").json()["version"]["version_id"] == 1

    ack(client, "dw-b", 1, first["sha256"])
    assert claim(client, "dw-b").json()["version"]["version_id"] == 2
    ack(client, "dw-b", 2, second["sha256"])
    ack(client, "dw-a", 2, second["sha256"])

    listing = client.get("/api/v1/delivery/consumers").json()["consumers"]
    by_id = {row["consumer_id"]: row for row in listing}
    assert set(by_id) == {"dw-a", "dw-b"}
    assert by_id["dw-a"]["next_version"] == 3
    assert by_id["dw-b"]["next_version"] == 3
    assert by_id["dw-a"]["last_claimed_version"] is None


def test_unknown_consumer_progress_is_404_and_list_is_empty(client: TestClient):
    response = client.get(PROGRESS.format(cid="ghost"))
    assert response.status_code == 404
    assert response.json()["error"] == "consumer_not_found"
    assert client.get("/api/v1/delivery/consumers").json() == {"consumers": []}


@pytest.mark.parametrize("bad_id", ["bad id", "éurope", "x" * 129, "-leading-dash", ".dot"])
def test_invalid_consumer_id_rejected(client: TestClient, bad_id: str):
    claimed = client.post(CLAIM.format(cid=bad_id))
    assert claimed.status_code == 400
    assert claimed.json()["error"] == "invalid_consumer_id"

    publish_two(client)
    acknowledged = client.post(
        ACK.format(cid=bad_id), json={"version_id": 1, "sha256": "a" * 64}
    )
    assert acknowledged.status_code == 400
    assert acknowledged.json()["error"] == "invalid_consumer_id"


@pytest.mark.parametrize(
    "bad_id", ["", " ", "slash/x", "../etc", "back\\slash", "under_score?"]
)
def test_invalid_consumer_id_rejected_at_store_layer(
    data_dir: Path, bad_id: str
):
    # Empty strings and values containing path separators never reach the path
    # operation (routing rejects them first), but the store must still refuse
    # them if called directly.
    store = Store(data_dir)
    store.init()
    from app.store import InvalidConsumer

    with pytest.raises(InvalidConsumer):
        store.claim_delivery(bad_id)
    with pytest.raises(InvalidConsumer):
        store.acknowledge_delivery(bad_id, 1, "a" * 64)
    with pytest.raises(InvalidConsumer):
        store.get_consumer(bad_id)


def test_malformed_ack_body_returns_422(client: TestClient):
    publish_two(client)
    claim(client, "dw-a")
    response = client.post(ACK.format(cid="dw-a"), json={"version_id": 1})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Restart durability
# ---------------------------------------------------------------------------


def test_pending_claim_and_cursor_survive_restart(
    client: TestClient, data_dir: Path
):
    first, second = publish_two(client)
    claim(client, "dw-a")
    ack(client, "dw-a", 1, first["sha256"])
    claim(client, "dw-a")  # version 2 claimed, not confirmed -> pending
    claim(client, "dw-b")  # version 1 pending for B
    main_module._store = None

    restarted = Store(data_dir)
    restarted.init()
    main_module.set_store(restarted)
    with TestClient(main_module.app) as c:
        # A resumes exactly at the unacknowledged version 2.
        redelivered = claim(c, "dw-a").json()
        assert redelivered["version"]["version_id"] == 2
        assert redelivered["replayed"] is True
        assert redelivered["next_version"] == 2

        # B resumes at version 1.
        assert claim(c, "dw-b").json()["version"]["version_id"] == 1

        # Completing the workflow after restart works end to end.
        assert ack(c, "dw-a", 2, second["sha256"]).status_code == 200
        assert claim(c, "dw-a").json()["version"] is None
    main_module._store = None


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_claims_and_acks_never_skip_or_mix(client: TestClient):
    total = 6
    versions = [upload(client).json()["version"] for _ in range(total)]
    digests = {v["version_id"]: v["sha256"] for v in versions}

    consumer_ids = ["dw-a", "dw-b", "dw-c"]
    errors = []
    barrier = threading.Barrier(len(consumer_ids))

    def worker(cid: str):
        barrier.wait()
        try:
            seen = []
            while True:
                body = claim(client, cid).json()
                if body["version"] is None:
                    break
                version_id = body["version"]["version_id"]
                # Every delivery must be the earliest unconfirmed version.
                assert version_id == len(seen) + 1
                assert body["version"]["sha256"] == digests[version_id]
                seen.append(version_id)
                response = ack(client, cid, version_id, digests[version_id])
                assert response.status_code == 200, response.text
            assert seen == list(range(1, total + 1))
        except AssertionError as exc:  # pragma: no cover - failure reporting
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(cid,)) for cid in consumer_ids]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    listing = client.get("/api/v1/delivery/consumers").json()["consumers"]
    assert {row["consumer_id"] for row in listing} == set(consumer_ids)
    assert all(row["next_version"] == total + 1 for row in listing)
    assert all(row["last_claimed_version"] is None for row in listing)


def test_concurrent_publish_and_claim_never_delivers_a_half_version(
    client: TestClient,
):
    upload(client)  # version 1 so there is always something to claim
    stop = threading.Event()
    errors = []
    delivered_ids = []
    publish_count = 5
    total_versions = 1 + publish_count

    def publisher():
        for index in range(publish_count):
            files = dict(BASE_FILES)
            files["stops.txt"] = (
                "stop_id,stop_name\nS1,Central\nS2,People Square\nS3,Science Hall\n"
                f"S{200 + index},Extra {index}\n"
            )
            response = upload(client, zip_bytes(files))
            if response.status_code != 201:
                errors.append(response.text)
                return

    def consumer():
        cid = "racer"
        while not stop.is_set():
            body = claim(client, cid).json()
            if body["version"] is None:
                # Nothing pending right now; back off briefly unless told to stop.
                if stop.wait(timeout=0.01):
                    break
                continue
            info = body["version"]
            # A delivered version must be fully queryable (never half-published).
            stops = client.get(
                f"/api/v1/versions/{info['version_id']}/trips/T1/stops"
            )
            if stops.status_code != 200:
                errors.append(f"version {info['version_id']} not queryable")
                return
            delivered_ids.append(info["version_id"])
            ack_response = ack(client, cid, info["version_id"], info["sha256"])
            if ack_response.status_code != 200:
                errors.append(ack_response.text)
                return

    consumer_thread = threading.Thread(target=consumer)
    consumer_thread.start()
    try:
        publisher()
        # Let the consumer drain every committed version before signalling stop.
        deadline = 10.0
        waited = 0.0
        while waited < deadline:
            progress = client.get(PROGRESS.format(cid="racer")).json()
            if progress["next_version"] == total_versions + 1:
                break
            time.sleep(0.02)
            waited += 0.02
    finally:
        stop.set()
        consumer_thread.join()

    assert errors == []
    # The consumer progressed strictly in order, without gaps or duplicates.
    assert delivered_ids == sorted(delivered_ids)
    assert len(set(delivered_ids)) == len(delivered_ids)
    assert delivered_ids[0] == 1
    # Every published version (1..6) was delivered and confirmed exactly once.
    final = client.get(PROGRESS.format(cid="racer")).json()
    assert final["next_version"] == total_versions + 1
    assert final["last_claimed_version"] is None
    assert delivered_ids == list(range(1, total_versions + 1))


def test_repeated_ack_under_concurrency_confirms_once(client: TestClient):
    first, _ = publish_two(client)
    claim(client, "dw-a")
    outcomes = []
    barrier = threading.Barrier(4)

    def confirm():
        barrier.wait()
        outcomes.append(ack(client, "dw-a", 1, first["sha256"]).status_code)

    threads = [threading.Thread(target=confirm) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Every retry is accepted (one real advance, the rest idempotent)...
    assert outcomes == [200, 200, 200, 200]
    # ...but the cursor moved exactly once, to version 2.
    progress = client.get(PROGRESS.format(cid="dw-a")).json()
    assert progress["next_version"] == 2
    assert progress["last_claimed_version"] is None


def test_delivery_summary_matches_version_api(client: TestClient):
    first, _ = publish_two(client)
    delivered = claim(client, "dw-a").json()["version"]
    from_api = client.get("/api/v1/versions/1").json()
    assert delivered == from_api
    assert delivered["sha256"] == first["sha256"]


def test_zip_is_untrusted_but_delivery_still_works_with_real_archive(
    client: TestClient, tmp_path: Path
):
    """End-to-end using a freshly built archive written outside the data dir."""
    import io

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("stops.txt", BASE_FILES["stops.txt"])
        zf.writestr("trips.txt", BASE_FILES["trips.txt"])
        zf.writestr("stop_times.txt", BASE_FILES["stop_times.txt"])
    payload = buffer.getvalue()

    published = upload(client, payload).json()["version"]
    delivered = claim(client, "dw-a").json()
    assert delivered["version"]["version_id"] == 1
    assert delivered["version"]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert (
        client.get(delivered["download_url"]).content == payload
    )
    assert (
        ack(client, "dw-a", 1, published["sha256"]).json()["next_version"] == 2
    )
