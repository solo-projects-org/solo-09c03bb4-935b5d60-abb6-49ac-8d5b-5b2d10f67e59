# GTFS 时刻表发布服务

FastAPI + SQLite 实现的 GTFS 版本发布服务：上传 ZIP 包发布新版本，旧版本在新版本发布后仍可按版号查询和下载。

## 快速开始

```bash
docker compose up --build -d
open http://localhost:8000/docs          # Swagger UI
```

样例包位于 `samples/`：

```bash
# 发布第一版
curl -sS -X POST http://localhost:8000/api/v1/versions \
     -F "file=@samples/gtfs_v1.zip" | jq

# 发布第二版（上一版仍然保留）
curl -sS -X POST http://localhost:8000/api/v1/versions \
     -F "file=@samples/gtfs_v2.zip" | jq

# 查询当前版本某班次的完整停站
curl -sS "http://localhost:8000/api/v1/trips/T1/stops" | jq
# 按版号查询（旧版）
curl -sS "http://localhost:8000/api/v1/trips/T1/stops?version=1" | jq

# 下载原 ZIP
curl -sS -OJ "http://localhost:8000/api/v1/versions/1/download"
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/versions` | 上传 ZIP 并发布新版本 |
| `GET` | `/api/v1/versions` | 列出版本（新版本在前），含当前版本号 |
| `GET` | `/api/v1/versions/{version}` | 版本元信息（`version` 可为版号或 `current`） |
| `GET` | `/api/v1/versions/{version}/trips/{trip_id}/stops` | 指定版本某班次的完整停站 |
| `GET` | `/api/v1/trips/{trip_id}/stops?version=…` | 班次停站，`version` 省略时为当前版本 |
| `GET` | `/api/v1/versions/{version}/download` | 下载该版本原始 ZIP |
| `POST` | `/api/v1/delivery/consumers/{consumer_id}/claim` | 领取该消费者最早未确认的已提交版本 |
| `POST` | `/api/v1/delivery/consumers/{consumer_id}/ack` | 确认已领取版本（须同时提供版号与摘要） |
| `GET` | `/api/v1/delivery/consumers` | 列出所有消费者的交付进度 |
| `GET` | `/api/v1/delivery/consumers/{consumer_id}` | 查看单个消费者的交付进度 |
| `GET` | `/healthz` | 健康检查 |

Swagger：`/docs`，OpenAPI：`/openapi.json`。

## 增量交付 API（按消费者独立进度）

下游数仓通过“领取 → 处理 → 确认”循环，按发布顺序增量接收原包；每个 `consumer_id` 在服务端独立维护进度，互不影响。

```bash
# 1) 领取最早未确认的已提交版本（首次领取自动从第 1 版补齐）
curl -sS -X POST http://localhost:8000/api/v1/delivery/consumers/dw-01/claim | jq
# -> { "next_version": 1, "last_claimed_version": 1, "replayed": false,
#      "version": { "version_id": 1, "sha256": "…", … },
#      "download_url": "/api/v1/versions/1/download" }

# 2) 通过 download_url 下载原包并校验 sha256，然后确认（版号 + 摘要必须同时匹配）
curl -sS -X POST http://localhost:8000/api/v1/delivery/consumers/dw-01/ack \
     -H 'Content-Type: application/json' \
     -d '{"version_id": 1, "sha256": "<领取时返回的 sha256>"}'
# -> { "advanced": true, "next_version": 2, "last_claimed_version": null }
```

交付语义与保证：

- **按序补齐**：领取始终返回该消费者最早未确认的已提交版本。消费者在第 N 版发布后才首次接入，也会从第 1 版开始领取，不会跳过既有版本。
- **崩溃重取**：版本确认前反复领取，始终返回同一个版本（`replayed=true`）。消费者或服务重启后，待确认领取仍原样可重取，进度持久化在 SQLite 中。
- **确认才推进**：只有同时满足“版号等于当前已领取未确认版”“请求摘要等于发布时登记的 SHA-256”“磁盘原包存在且 SHA-256 校验通过”，游标才推进到下一版并清除待确认标记，全部在一个 SQLite 写事务内原子完成。
- **不能跳版/串用**：确认未来版号返回 `409 ack_version_mismatch`；不同 `consumer_id` 的游标完全隔离。
- **重复确认幂等**：对已确认版本的重复确认返回 `200` 且 `advanced=false`，游标不再移动，因此重复确认绝不会跳过下一版。
- **原包异常可恢复**：确认时原包缺失返回 `410 artifact_missing`、摘要不符返回 `409 artifact_digest_mismatch`，游标保持不动；运维修复原包后用相同的领取/确认请求即可继续。
- **全部确认完**：领取返回 `version=null`、`download_url=null`；之后发布的新版本会在下一次领取时交付。
- **并发安全**：领取与确认均在 `BEGIN IMMEDIATE` 写事务中执行，多实例并发发布、领取、确认时由 SQLite 写锁串行化，不会交付半版、越过旧版或串用其他消费者的进度。

消费者 ID 规则：1–128 字符，首字符为字母或数字，其余可为字母、数字、`_`、`.`、`-`；不合法返回 `400 invalid_consumer_id`。

| 场景 | 状态码 | `error` |
| --- | --- | --- |
| 消费者 ID 不合法 | 400 | `invalid_consumer_id` |
| 对从未领取的消费者确认 | 404 | `consumer_not_found` |
| 确认版号与待确认版不符（含跳版） | 409 | `ack_version_mismatch` / `ack_without_claim` |
| 确认摘要与登记摘要不符 | 409 | `ack_digest_mismatch` |
| 原包缺失 | 410 | `artifact_missing` |
| 原包摘要校验失败 | 409 | `artifact_digest_mismatch` |

## 发布校验

上传的 ZIP 必须包含 `stops.txt`、`trips.txt`、`stop_times.txt`（UTF-8 CSV）：

- `stops.txt`：`stop_id` 必须唯一，`stop_name` 必填。
- `trips.txt`：`trip_id` 必须唯一。
- `stop_times.txt`：每行必须引用存在的 `stop_id` 与 `trip_id`，`stop_sequence` 为非负整数且同班次内不重复，`arrival_time` 必填；
  按站序排列时到站时刻不得倒退（相等允许）。
- 时刻格式 `HH:MM:SS`，小时可超过 24（例如 `25:10:00` 表示次日 01:10）。
- 每个班次至少要有一个停站。

校验失败返回 `422`，`errors[]` 每项含 `file` 与 `line`（文件内 1 起始的物理行号，含表头行），例如：

```json
{
  "error": "validation_failed",
  "message": "2 validation error(s); no version was published",
  "errors": [
    {"file": "stop_times.txt", "line": 7, "code": "unknown_stop_id",
     "message": "stop_id 'S99' is not defined in stops.txt", "value": "S99"}
  ]
}
```

不合格的包不会产生任何版本，已有版本完全不受影响（`GET /api/v1/versions` 不变）。

## 一致性保证

- 上传字节先写入 `staging/` 唯一临时文件并完成校验，再在 SQLite 写事务（`BEGIN IMMEDIATE`）内原子改名为不可变的 `artifacts/<uuid>.zip`，最后在同一事务里登记版本及所有行并提交。并发上传因此串行化。
- 读者在单个读事务中解析版本号并读取数据，因此每次查询只会看到某一个已提交版本，不会看到半个版本或跨版本混合。
- 登记失败会回滚事务并删除刚落地的文件；启动时还会清理残留的临时文件与未被引用的包（保留已提交版本的文件），重启不丢版本。
- 下载前校验文件存在且 SHA-256 与登记的摘要一致，并用同一文件句柄流传已校验的字节；缺失返回 `410`，摘要不符返回 `409`。
- 新版本发布只追加，旧版号仍可查询与下载。发布响应中的 `previous_version` 在同一写事务内读取，并发上传时也准确。
- 增量交付的领取与确认各自在一个 `BEGIN IMMEDIATE` 写事务中完成：消费者游标的读取/更新与版本行的读取处于同一事务快照，因此多实例并发下每个消费者都严格按版号顺序交付，看不到半版也不会越过旧版；确认对原包的完整性校验与游标推进同事务原子生效。
- 消费者进度（`consumer_progress` 表）与版本数据在同一个 SQLite 数据库中持久化，服务重启后待确认领取可原样重取、已确认游标不丢失。

## 数据

- 容器数据目录 `/data`（Docker volume `gtfs-data`）：`gtfs.db`、`artifacts/`、`staging/`。
- 环境变量 `GTFS_DATA_DIR` 可改数据目录，`GTFS_MAX_UPLOAD_BYTES` 可改上传大小上限（默认 256 MiB）。

## 本地运行与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
uvicorn app.main:app --reload
pytest
```

## 样例包

| 文件 | 说明 |
| --- | --- |
| `samples/gtfs_v1.zip` | 2 条线路、3 个班次；包含跨午夜 `24:xx` 时刻；站序故意不连续（10/20/30） |
| `samples/gtfs_v2.zip` | 第二版：新增站点、新增班次、调整时刻；同一 `trip_id` 的停站与 v1 有差异，便于验证按版号隔离 |

`gtfs_v2.zip` 中 `T1` 的时刻与站点顺序和 v1 不同，可用来确认 `?version=1` 与 `?version=2` 返回各自版本的数据。
