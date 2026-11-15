"""FastAPI application exposing the GTFS version publication API.

Guarantees provided to readers:

* Every request resolves versions and reads rows inside a single SQLite read
  transaction, so it observes one committed version — never a partially
  published one, and never a mix of two.
* A publication becomes visible all at once when its SQLite transaction
  commits; the previous version remains intact and queryable by version id.
* Uploaded bytes are staged and validated before the artifact is renamed into
  place; a failed registration rolls the artifact back, leaving no residue and
  no damage to existing versions.
* Downloads are served from the same open descriptor whose SHA-256 was checked
  against the registered digest, so a missing or tampered artifact is refused
  rather than served.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .gtfs_parse import FeedRejected, validate_archive
from .store import (
    ClaimResult,
    StorageError,
    Store,
    VersionInfo,
    iter_fd,
)

logger = logging.getLogger("gtfs.publisher")

DEFAULT_MAX_UPLOAD_BYTES = 256 * 1024 * 1024
UPLOAD_CHUNK = 1 * 1024 * 1024


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    get_store()  # opens/creates the DB and runs startup recovery
    yield


app = FastAPI(
    title="GTFS 时刻表发布服务",
    description=(
        "上传 GTFS ZIP 包（stops.txt、trips.txt、stop_times.txt）发布为新版本；"
        "旧版本在新版本发布后仍可按版号查询与下载。\n\n"
        "发布时校验：站点/班次 ID 唯一、停站必须引用存在的站点和班次、"
        "每站必须有到站时刻、同班次站序不得重复、时刻随站序不得倒退；"
        "超过 24 点的 GTFS 时刻（如 25:10:00）是合法的。"
        "校验失败会返回文件与行号，且不会改变任何已发布版本。\n\n"
        "增量交付 API（/api/v1/delivery/...）按消费者 ID 独立维护进度："
        "领取最早未确认的已提交版本，返回版号、摘要与原包下载地址；"
        "只有同时确认版号与摘要（且原包通过完整性校验）才推进游标，"
        "未确认时反复领取仍返回同一版，崩溃后可重取，重复确认不会跳过下一版。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class VersionSummary(BaseModel):
    version_id: int = Field(description="版本号，发布后单调递增")
    created_at: str = Field(description="发布时间（UTC, ISO 8601）")
    upload_filename: str = Field(description="上传时的原始文件名")
    sha256: str = Field(description="ZIP 包 SHA-256 摘要")
    size_bytes: int = Field(description="ZIP 包字节数")
    stops_count: int
    trips_count: int
    stop_times_count: int


class VersionList(BaseModel):
    current_version: Optional[int] = Field(description="当前版本号，尚无版本时为 null")
    versions: List[VersionSummary]


class PublicationResult(BaseModel):
    version: VersionSummary
    previous_version: Optional[int] = Field(
        description="发布前查询的当前版本号（旧版仍可按版号访问）"
    )


class StopArrival(BaseModel):
    stop_sequence: int
    stop_id: str
    stop_name: str
    arrival_time: str = Field(description="GTFS 原始到站时刻，可超过 24:00:00")
    arrival_seconds: int = Field(description="自服务日起点的秒数")


class TripStops(BaseModel):
    version_id: int
    trip_id: str
    route_id: Optional[str]
    stops: List[StopArrival]


class ValidationFailure(BaseModel):
    error: str
    message: str
    errors: List[dict]


class ErrorResponse(BaseModel):
    error: str
    message: str


# ---------------------------------------------------------------------------
# Delivery (incremental consumer API) schemas
# ---------------------------------------------------------------------------


class AckRequest(BaseModel):
    version_id: int = Field(gt=0, description="已领取并完成处理的版本号")
    sha256: str = Field(
        min_length=1,
        description="领取时返回的该版本 ZIP 包 SHA-256 摘要，必须与登记摘要一致",
    )


class DeliveryEnvelope(BaseModel):
    consumer_id: str = Field(description="消费者 ID，各消费者进度互不影响")
    next_version: int = Field(description="该消费者最早未确认的版本号（交付游标）")
    last_claimed_version: Optional[int] = Field(
        description="当前已领取但尚未确认的版本号；无待确认领取时为 null"
    )
    replayed: bool = Field(
        description="true 表示这是同一未确认版号的重复/崩溃后重取，而非新领取"
    )
    version: Optional[VersionSummary] = Field(
        description="待交付版本摘要；所有已提交版本均确认完毕时为 null"
    )
    download_url: Optional[str] = Field(
        description="该版本现有原包的下载地址（相对路径）；无待交付版本时为 null"
    )


class AckResponse(BaseModel):
    consumer_id: str
    version_id: int = Field(description="本次确认的版本号")
    advanced: bool = Field(
        description="true 表示游标本次实际推进；false 表示幂等的重复确认，未推进"
    )
    next_version: int = Field(description="确认后该消费者的下一个待交付版本号")
    last_claimed_version: Optional[int]


class ConsumerProgress(BaseModel):
    consumer_id: str
    next_version: int
    last_claimed_version: Optional[int]
    created_at: str
    updated_at: str


class ConsumerList(BaseModel):
    consumers: List[ConsumerProgress]


# ---------------------------------------------------------------------------
# Application state / wiring
# ---------------------------------------------------------------------------


def _data_dir() -> Path:
    return Path(os.environ.get("GTFS_DATA_DIR", "/data"))


_store: Optional[Store] = None


def get_store() -> Store:
    global _store
    if _store is None:
        store = Store(_data_dir())
        store.init()
        _store = store
    return _store


def set_store(store: Store) -> None:
    """Used by tests to inject an isolated store."""
    global _store
    _store = store


def max_upload_bytes() -> int:
    raw = os.environ.get("GTFS_MAX_UPLOAD_BYTES")
    if raw is None:
        return DEFAULT_MAX_UPLOAD_BYTES
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_UPLOAD_BYTES
    return value if value > 0 else DEFAULT_MAX_UPLOAD_BYTES


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@app.exception_handler(StorageError)
async def _storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
    headers = {}
    if exc.status_code == 410:
        headers["Cache-Control"] = "no-store"
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "message": exc.message},
        headers=headers,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _summary(info: VersionInfo) -> VersionSummary:
    return VersionSummary(
        version_id=info.version_id,
        created_at=info.created_at,
        upload_filename=info.upload_filename,
        sha256=info.sha256,
        size_bytes=info.size_bytes,
        stops_count=info.stops_count,
        trips_count=info.trips_count,
        stop_times_count=info.stop_times_count,
    )


def _safe_upload_name(raw: Optional[str]) -> str:
    name = (raw or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not name:
        name = "feed.zip"
    # Keep the persisted name printable and bounded; it is never used as a path.
    name = "".join(ch for ch in name if ch >= " " and ch != "\x7f")
    return name[:200]


def _invalid_upload(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": "validation_failed",
            "message": message,
            "errors": [
                {
                    "file": "",
                    "line": 0,
                    "code": "invalid_archive",
                    "message": message,
                }
            ],
        },
    )


# ---------------------------------------------------------------------------
# Publication
# ---------------------------------------------------------------------------


@app.post(
    "/api/v1/versions",
    response_model=PublicationResult,
    status_code=201,
    summary="发布新的 GTFS 版本",
    responses={
        422: {
            "model": ValidationFailure,
            "description": "GTFS 校验失败，errors 中包含文件与行号",
        },
        413: {"model": ErrorResponse, "description": "上传包超过大小限制"},
    },
)
async def publish_version(
    file: UploadFile = File(..., description="包含 stops.txt、trips.txt、stop_times.txt 的 ZIP"),
) -> JSONResponse:
    store = get_store()
    upload_name = _safe_upload_name(file.filename)
    limit = max_upload_bytes()

    # Stage the upload under a unique name; nothing is registered yet.
    fd, staging_name = tempfile.mkstemp(prefix="upload-", suffix=".zip", dir=store.staging_dir)
    staging_path = Path(staging_name)
    sha256 = hashlib.sha256()
    size = 0
    try:
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > limit:
                        return JSONResponse(
                            status_code=413,
                            content={
                                "error": "upload_too_large",
                                "message": f"upload exceeds the {limit} byte limit",
                            },
                        )
                    sha256.update(chunk)
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
        finally:
            await file.close()

        if size == 0:
            return _invalid_upload("uploaded file is empty")

        # Validation and registration are blocking (file IO, SQLite write lock),
        # so they run off the event loop; queries stay responsive meanwhile.
        try:
            info, previous = await run_in_threadpool(
                _validate_and_publish,
                store,
                staging_path,
                sha256.hexdigest(),
                size,
                upload_name,
            )
        except FeedRejected as rejected:
            return JSONResponse(
                status_code=422,
                content={
                    "error": "validation_failed",
                    "message": f"{len(rejected.errors)} validation error(s); "
                    "no version was published",
                    "errors": rejected.errors,
                },
            )
        except zipfile.BadZipFile:
            return _invalid_upload("uploaded file is not a valid ZIP archive")
        except Exception:
            logger.exception("publication failed")
            # Registration failed; publish() already removed the artifact.
            return JSONResponse(
                status_code=500,
                content={
                    "error": "publication_failed",
                    "message": "the feed could not be registered; the previous "
                    "version is unchanged",
                },
            )
        staging_path = None  # ownership moved into the store
        return JSONResponse(
            status_code=201,
            content={
                "version": _summary(info).model_dump(),
                "previous_version": previous,
            },
        )
    finally:
        if staging_path is not None:
            try:
                staging_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _validate_and_publish(
    store: Store,
    staging_path: Path,
    sha256: str,
    size: int,
    upload_name: str,
):
    """Blocking part of the publish path, executed in a worker thread."""
    with zipfile.ZipFile(staging_path, "r") as zf:
        feed = validate_archive(zf, upload_name)
    return store.publish(
        staging_path=staging_path,
        sha256=sha256,
        size_bytes=size,
        upload_filename=upload_name,
        feed=feed,
    )


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/versions",
    response_model=VersionList,
    summary="列出版本（新版本在前）",
)
def list_versions() -> VersionList:
    store = get_store()
    versions, current = store.list_versions()
    return VersionList(
        current_version=current, versions=[_summary(v) for v in versions]
    )


@app.get(
    "/api/v1/versions/{version}",
    response_model=VersionSummary,
    summary="查看某个版本的信息",
    responses={404: {"model": ErrorResponse}},
)
def get_version(version: str) -> VersionSummary:
    info, _current = get_store().get_version(version)
    return _summary(info)


@app.get(
    "/api/v1/versions/{version}/trips/{trip_id}/stops",
    response_model=TripStops,
    summary="按版本号或 current 查询某班次的完整停站",
    responses={404: {"model": ErrorResponse}},
)
def trip_stops_by_version(version: str, trip_id: str) -> TripStops:
    info, route_id, stops = get_store().trip_stops(version, trip_id)
    return TripStops(
        version_id=info.version_id, trip_id=trip_id, route_id=route_id, stops=stops
    )


@app.get(
    "/api/v1/trips/{trip_id}/stops",
    response_model=TripStops,
    summary="查询班次停站（version 省略时使用当前版本）",
    responses={404: {"model": ErrorResponse}},
)
def trip_stops_current(
    trip_id: str,
    version: str = Query(
        "current",
        description="版本号或 current（默认）",
        examples=["current", "1"],
    ),
) -> TripStops:
    info, route_id, stops = get_store().trip_stops(version, trip_id)
    return TripStops(
        version_id=info.version_id, trip_id=trip_id, route_id=route_id, stops=stops
    )


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _download_headers(info: VersionInfo) -> dict:
    return {
        "Content-Disposition": f'attachment; filename="gtfs-version-{info.version_id}.zip"',
        "Content-Length": str(info.size_bytes),
        "Content-Type": "application/zip",
        "ETag": f'"{info.sha256}"',
        "X-GTFS-Version": str(info.version_id),
        "X-Content-SHA256": info.sha256,
        "Cache-Control": "no-store",
    }


@app.get(
    "/api/v1/versions/{version}/download",
    summary="下载该版本原始 ZIP（校验摘要后返回）",
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse, "description": "摘要不符"},
        410: {"model": ErrorResponse, "description": "文件缺失"},
    },
)
def download_version(version: str):
    store = get_store()
    info, _current = store.get_version(version)
    # Verify the whole artifact before any bytes go on the wire; the returned
    # descriptor is the one that gets streamed, so the bytes cannot change
    # between verification and transmission.  StorageError subclasses
    # (missing -> 410, digest mismatch -> 409) are handled by the global handler.
    fd = store.open_verified_artifact(info)

    filename = f"gtfs-version-{info.version_id}.zip"
    headers = _download_headers(info)
    headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return StreamingResponse(
        iter_fd(fd, info.size_bytes),
        media_type="application/zip",
        headers=headers,
    )


@app.get("/healthz", summary="健康检查")
def healthz() -> dict:
    store = get_store()
    try:
        versions, current = store.list_versions()
    except Exception as exc:  # pragma: no cover - defensive
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(exc)},
        )
    return {"status": "ok", "versions": len(versions), "current_version": current}


# ---------------------------------------------------------------------------
# Incremental delivery (per-consumer progress)
# ---------------------------------------------------------------------------


def _delivery_envelope(result: ClaimResult) -> DeliveryEnvelope:
    summary = _summary(result.info) if result.info is not None else None
    download_url = (
        f"/api/v1/versions/{result.info.version_id}/download"
        if result.info is not None
        else None
    )
    # Whenever a version is returned it is the consumer's pending claim
    # (fresh or replayed); when nothing is deliverable there is no pending slot.
    pending = result.info.version_id if result.info is not None else None
    return DeliveryEnvelope(
        consumer_id=result.consumer_id,
        next_version=result.next_version,
        last_claimed_version=pending,
        replayed=result.replayed,
        version=summary,
        download_url=download_url,
    )


@app.post(
    "/api/v1/delivery/consumers/{consumer_id}/claim",
    response_model=DeliveryEnvelope,
    summary="领取该消费者最早未确认的已提交版本",
    responses={
        200: {
            "description": "返回待交付版本；所有版本均已确认时 version 为 null",
        },
        400: {"model": ErrorResponse, "description": "消费者 ID 不合法"},
    },
)
def claim_delivery(consumer_id: str) -> DeliveryEnvelope:
    """按发布顺序交付下一个版本。

    * 首次领取自动从最早的已提交版本开始（补齐既有版本）。
    * 上一版尚未确认时反复领取，始终返回同一个版本（``replayed=true``），
      因此消费者崩溃重启后可以原样重取。
    * 所有已提交版本都确认完毕时返回 ``version=null``，新发布后再次领取即可。
    * 每个消费者 ID 独立维护游标，互不串用；整个领取在一个 SQLite 写事务中
      完成，多实例并发时不会交付半版或越过旧版。
    """
    return _delivery_envelope(get_store().claim_delivery(consumer_id))


@app.post(
    "/api/v1/delivery/consumers/{consumer_id}/ack",
    response_model=AckResponse,
    status_code=200,
    summary="确认已领取版本（须同时提供版号与摘要）",
    responses={
        400: {"model": ErrorResponse, "description": "消费者 ID 或版号不合法"},
        404: {"model": ErrorResponse, "description": "消费者尚未领取过任何版本"},
        409: {
            "model": ErrorResponse,
            "description": "版号与待确认版不符、摘要不符，或原包校验失败",
        },
        410: {"model": ErrorResponse, "description": "原包缺失，修复后可重试"},
    },
)
def acknowledge_delivery(consumer_id: str, payload: AckRequest) -> AckResponse:
    """只有版号和摘要都匹配、且原包完整时才推进到下一版。

    * 版号必须等于该消费者当前已领取未确认的版本，否则返回 409，游标不动。
    * 摘要必须与发布时登记的 SHA-256 一致，否则返回 409，游标不动。
    * 确认时会现场校验磁盘上的原包：缺失返回 410、摘要不符返回 409；
      运维修复原包后用相同的领取/确认请求即可继续。
    * 对已确认版本的重复确认幂等成功（``advanced=false``），绝不会跳过下一版。
    """
    result = get_store().acknowledge_delivery(
        consumer_id=consumer_id,
        version_id=payload.version_id,
        sha256=payload.sha256,
    )
    return AckResponse(
        consumer_id=result.consumer_id,
        version_id=result.version_id,
        advanced=result.advanced,
        next_version=result.next_version,
        last_claimed_version=result.last_claimed_version,
    )


@app.get(
    "/api/v1/delivery/consumers",
    response_model=ConsumerList,
    summary="列出所有消费者的交付进度",
)
def list_consumers() -> ConsumerList:
    states = get_store().list_consumers()
    return ConsumerList(
        consumers=[
            ConsumerProgress(
                consumer_id=state.consumer_id,
                next_version=state.next_version,
                last_claimed_version=state.last_claimed_version,
                created_at=state.created_at,
                updated_at=state.updated_at,
            )
            for state in states
        ]
    )


@app.get(
    "/api/v1/delivery/consumers/{consumer_id}",
    response_model=ConsumerProgress,
    summary="查看单个消费者的交付进度",
    responses={404: {"model": ErrorResponse}},
)
def get_consumer_progress(consumer_id: str) -> ConsumerProgress:
    state = get_store().get_consumer(consumer_id)
    return ConsumerProgress(
        consumer_id=state.consumer_id,
        next_version=state.next_version,
        last_claimed_version=state.last_claimed_version,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )
