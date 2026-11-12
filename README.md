# Multi-Center DICOM Ingest API

多中心 DICOM 入库与后台补采平台。解决两个核心问题：

1. **设备在不同中心复用同一个 DICOM UID** 时，分割/引用结果可能错误地指向
   别家中心的影像。本平台对每个来源中心独立、确定性地重映射所有实例标识，
   使跨中心数据在物理上无法串联。
2. **从中心影像服务后台补采指定研究**：按游标逐页拉取 SOP 清单与 DICOM 对象，
   沿用同一套 UID 改写与原子入库规则，支持断点续采、故障暂停/重试与并发安全。

技术栈：**FastAPI + pydicom + SQLite（WAL 模式）+ httpx 后台 worker**，
Docker Compose 一键启动两个服务（入库 API + 本地影像源），并提供交互式
API 文档（Swagger UI）。

---

## 1. 核心保证

| 需求 | 实现方式 |
| --- | --- |
| 按“中心 + 原 UID”稳定生成新 UID | `new_uid = "2.25." + uuid5(固定命名空间, center + "|" + 原UID).int`，纯函数，无需查库、跨进程/重启不变（`app/uid.py`） |
| 跨中心同原 UID 不碰撞 | 中心标识参与哈希输入；同原 UID 在不同中心必然得到不同新 UID，引用永不跨中心 |
| 统一改写范围 | 顶层 Study/Series/SOP Instance UID、文件头 `MediaStorageSOPInstanceUID`、任意序列深度嵌套的 `ReferencedSOPInstanceUID`、序列内 `SeriesInstanceUID`、`FrameOfReferenceUID`、`ReferencedFrameOfReferenceUID`（递归遍历，`app/dicom_io.py`） |
| 不改写的内容 | SOP Class UID / Transfer Syntax UID 等“类别型”UID、Modality 等属性、**PixelData 像素字节**（写入后回读断言验证） |
| 仅接受 Explicit VR Little Endian | 校验文件头传输语法必须为 `1.2.840.10008.1.2.1`，否则整批 422 拒绝 |
| 引用先到、影像后到 | 引用先入库时在 `uid_mappings` 预留映射并在 `instance_references` 标记未解析；后续同中心影像到达后批量重新解析；不同中心到达的影像不会解析它 |
| 同中心同原 SOP 同字节重传 | 命中 `(center, original_sop_uid, sha256)` 唯一约束，直接复用已存对象，不产生重复行/重复文件 |
| 异字节冲突 | 同中心同原 SOP UID 但 SHA-256 不同 → 整批 409 拒绝 |
| 任一文件失败整批不入库 | 全部文件先在事务外解析；入库在单个 `BEGIN IMMEDIATE` 事务内完成，失败回滚，并清理已落盘的临时对象，**不存在半批可下载** |
| 并发安全 | SQLite WAL + 写事务互斥；唯一约束兜底；12 路并发实测全部 201 |
| 重启不变 | 映射为纯函数可离线复算，数据库与对象文件落盘到挂载卷 |
| 批量上限 | 每批最多 20 份文件（可配置） |
| 后台补采 | 提交中心、源地址、原 Study UID；worker 按游标逐页拉取并原子入库，支持暂停/续采/重启接续 |
| 补采原子性 | 每页的实例、引用、批次行与游标推进在**同一个 `BEGIN IMMEDIATE` 事务**内共同生效；坏文件/下载失败不留下半页、不推进游标 |
| 远端读取不占写事务 | 清单获取、文件下载、DICOM 解析全部在 SQLite 写事务外完成；原上传与查询在补采期间完全可用 |
| 补采幂等 | 同字节实例复用、异字节冲突暂停该页且可重试；页提交后立即中断或并发续采都不会重复建批 |

---

## 1a. 后台补采保证

| 需求 | 实现方式 |
| --- | --- |
| 游标清单协议 | `GET {源}/manifest?study_uid=&cursor=&page_size=` 返回 `items[{sop_uid,file_url}]` 与 `next_cursor`；空游标表示结束，每页 1–20 份，作业期间内容不变 |
| 作业状态机 | `queued → running →（paused/failed ↔ queued）→ completed`；状态持久化在 `backfill_jobs`，worker 用原子 `UPDATE ... WHERE status='queued'` 认领，单作业同时只有一个执行者 |
| 逐页核对 | 下载后校验 Explicit VR Little Endian、清单 SOP UID 与 DICOM `SOPInstanceUID` 一致、`StudyInstanceUID` 与请求的原 Study UID 一致，再走既有改写规则 |
| 故障暂停 | 远端不可达、HTTP 错误、断流、坏 DICOM、UID 不符、异字节冲突均暂停作业并保留旧游标；错误码与消息记录在作业和 `backfill_job_attempts` 历史中 |
| 可重试 | `POST /api/v1/backfills/{id}/resume` 把暂停/失败作业重新排队，从持久化游标重读同一页；故障清除后即可继续 |
| 重启接续 | 启动时把残留的 `running` 行（上次进程崩溃）重新排队；已提交页的游标即唯一续采状态，重跑不会产生第二个批次 |
| 并发续采 | 8 路并发 resume 只有一次排队生效；页提交在单写事务内并带作业状态校验，重复提交被约束与状态检查双重拦截 |
| 同字节复用 / 异字节冲突 | 补采页与上传共用 `service` 层：`(center, original_sop_uid, sha256)` 命中即复用，SHA 不同则整页回滚暂停 |
| 进度/错误接口 | 作业详情含游标、已提交页数/实例数/复用数、最后批次与最后错误；`/attempts` 返回每次页尝试（含失败原因） |

---

## 2. 目录结构

```
app/
  config.py        环境变量配置（数据目录、UID 根、批量上限、worker 参数等）
  uid.py           确定性 UID 重映射（uuid5 + 2.25 根）
  dicom_io.py      DICOM 解析/校验、递归 UID 改写、序列化后回读校验
  db.py            SQLite schema、WAL 连接、即时写事务、幂等列迁移
  service.py       解析/提交两阶段：批次原子入库、重传复用、字节冲突、引用解析、补采页事务
  remote.py        远端清单/对象 HTTP 客户端（httpx，超时与错误码）
  backfill.py      后台 worker、作业状态机、游标推进、暂停/续采/重启恢复
  main.py          FastAPI 路由与响应模型（上传、查询、补采）
source_app/        本地中心影像源（可分页清单 + DICOM 下载 + 故障注入）
  config.py        源数据目录与故障文件位置
  registry.py      启动时扫描研究，提供稳定游标分页
  faults.py        truncate/corrupt/http_500/disconnect 故障开关
  main.py          源服务 FastAPI 路由
scripts/
  generate_samples.py  生成合成样例（含 46 实例多页补采研究，--source-data）
  demo.sh              对运行中的服务执行上传/改写完整 HTTP 演示
  demo_backfill.sh     对运行中的双服务执行补采/断流/冲突/续采演示
tests/             37 项 pytest（上传、并发、补采、原子性、重启接续、故障注入）
samples/generated/ 合成 DICOM 样例（构建时生成）
Dockerfile, Dockerfile.source, docker-compose.yml, docker/entrypoint.sh
```

---

## 3. 快速开始（Docker Compose）

```bash
# 1. 构建并启动两个服务（样例与源研究在镜像构建时生成）
docker compose up --build -d

# 2. 健康检查
curl http://localhost:8000/healthz   # 入库 API + 补采 worker
curl http://localhost:8100/healthz   # 本地中心影像源

# 3. 打开交互式 API 文档
#    入库 API:   http://localhost:8000/docs
#    影像源:     http://localhost:8100/docs
```

### 在 Swagger UI 中演示后台补采

1. 打开 `http://localhost:8000/docs`，展开 `POST /api/v1/backfills` → **Try it out**
2. 请求体使用（源地址为 Compose 内的服务名，本机演示也可用 `http://localhost:8100`）：
   ```json
   {
     "center_id": "demo-center",
     "original_study_uid": "1.9.84.20261112.80.1",
     "source_base_url": "http://localhost:8100",
     "page_size": 10
   }
   ```
3. **Execute** 返回 201 与 `job_id`，worker 随即在后台逐页补采
4. 用 `GET /api/v1/backfills/{job_id}` 观察 `status` 从 `queued/running` 变为
   `completed`，`pages_committed` 与 `instances_committed` 增长，`cursor` 最终为空
5. 在 `http://localhost:8100/docs` 调 `POST /faults` 注入
   `truncate` / `corrupt` / `http_500` / `disconnect` 故障，新建补采作业可观察
   作业 `paused`、错误码、零半页；清除故障后调
   `POST /api/v1/backfills/{job_id}/resume` 续采至完成
6. 用 `GET /api/v1/backfills/{job_id}/attempts` 查看页尝试历史，用
   `GET /api/v1/instances` 与 `GET /api/v1/instances/{id}/download`
   读取改写后的 DICOM

### 一键脚本演示

```bash
# 上传/改写/引用/冲突等既有场景
BASE_URL=http://localhost:8000 bash scripts/demo.sh

# 补采全流程：分页、断流暂停、续采、异字节冲突、复用、下载校验
BASE_URL=http://localhost:8000 SOURCE_URL=http://localhost:8100 \
  bash scripts/demo_backfill.sh
```

### 不用 Docker 的本地运行

```bash
pip install -r requirements.txt
python3 scripts/generate_samples.py
python3 scripts/generate_samples.py --source-data ./source-data

# 终端 1：本地影像源
SOURCE_SERVICE_DATA_DIR=./source-data \
  uvicorn source_app.main:app --host 0.0.0.0 --port 8100

# 终端 2：入库 API + 补采 worker
DICOM_INGEST_DATA_DIR=./data \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 4. HTTP API 一览

入库 API（`http://localhost:8000`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | multipart 表单：`center_id` + `files`（1–20），原子入库 |
| GET | `/api/v1/batches` | 批次列表，可按 `center_id` 过滤（含补采产生的批次，带 `origin_kind`） |
| GET | `/api/v1/batches/{batch_id}` | 批次详情 + 每个文件的原/新 UID 映射 |
| GET | `/api/v1/instances` | 实例检索（`center_id` / `original_sop_uid` / `new_sop_uid`） |
| GET | `/api/v1/instances/{id}/download` | 下载改写后的 DICOM（`application/dicom`） |
| GET | `/api/v1/references/pending` | 某中心的引用解析状态（`resolved=true/false`） |
| POST | `/api/v1/backfills` | 提交补采作业（中心、原 Study UID、源地址、页大小 1–20） |
| GET | `/api/v1/backfills` | 补采作业列表，可按 `center_id` 过滤 |
| GET | `/api/v1/backfills/{job_id}` | 作业进度：状态、游标、页数/实例数/复用数、最后错误 |
| GET | `/api/v1/backfills/{job_id}/attempts` | 每页尝试历史（提交/暂停/失败与错误码） |
| POST | `/api/v1/backfills/{job_id}/resume` | 暂停/失败作业从持久化游标续采 |
| POST | `/api/v1/backfills/{job_id}/pause` | 请求暂停（在飞页仍原子提交完） |
| GET | `/healthz` | 存活探针 |

本地影像源（`http://localhost:8100`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/manifest` | 游标分页清单：`study_uid`、`cursor`、`page_size`（1–20） |
| GET | `/objects/{object_id}` | 下载 DICOM 对象（感知故障注入） |
| GET/POST/DELETE | `/faults` | 查询/注入/清除 `truncate`、`corrupt`、`http_500`、`disconnect` 故障 |
| GET | `/healthz` | 存活探针 |

错误语义：`422` = 文件/参数不合法（含非法 DICOM、传输语法不符、超 20 份）；
`409` = 与既有状态冲突（异字节同 UID、重复补采作业、库级唯一约束冲突）；
`404` = 批次/实例/作业不存在。补采运行期错误不使用 HTTP 错误码，而是把作业
置为 `paused`/`failed` 并在 `last_error_code` 给出稳定错误码
（`manifest_*`、`download_*`、`bad_dicom`、`uid_mismatch`、`byte_conflict` 等）。

---

## 5. 合成样例说明

`scripts/generate_samples.py` 生成的全部对象均为带 DICM 前导、文件头和真实
PixelData 的合法 Explicit VR Little Endian 文件：

- `cross_center_uid_collision/center-{a,b}/image_001.dcm`
  两个中心**复用完全相同的 Study/Series/SOP UID**，但像素值分别为 1000 与
  2000（异字节）。入库后得到两个不同的新 SOP UID，像素各自保持不变。
- `reference_first/struct_001.dcm`
  SR 对象，含三层嵌套
  （`ReferencedSOPSequence → ReferencedSeriesSequence → ReferencedSOPSequence`），
  引用一个尚未上传的影像 SOP。先上传它会产生一条未解析引用。
- `reference_first/image_001.dcm`
  被引用的影像。同中心上传后引用自动解析；别的中心上传同原 UID 影像不会解析。
- `invalid/not_a_dicom.dcm`
  非 DICOM 字节，用于演示整批拒绝。
- `backfill_study/`（45 张 CT + 1 个 SR）
  一个跨多页的研究（Study UID `1.9.84.20261112.80.1`），构建时同时落到
  源服务卷 `source-data/studies/<study_uid>/`，供游标分页与补采演示；SR 引用
  最后一张影像，验证补采路径同样执行嵌套引用改写。

---

## 6. 数据模型要点

- `uid_mappings`：每个 `(center, 原UID)` 一行，`present` 标识携带该 SOP 的实体
  是否已到达，支撑引用先到。
- `instances`：每个入库对象一行，含原/新三套 UID、SHA-256、存储相对路径，
  `(center, original_sop_uid, sha256)` 与 `new_sop_uid` 均唯一。
- `instance_references`：引用关系与解析状态，按中心隔离。
- `batches`：仅记录成功提交的批次；被拒绝的批次不留任何记录或对象。
  补采产生的批次通过 `origin_kind='backfill'`、`origin_job_id`、`page_no`、
  `cursor` 与作业关联。
- `backfill_jobs`：每个补采作业一行，持有作业状态机、当前游标、页/实例计数、
  复用数、最后批次与最后错误；`UNIQUE(center_id, original_study_uid)` 保证同一
  中心同一研究只有一个补采作业。
- `backfill_job_attempts`：每次页尝试（含暂停/失败及其错误码），用于审计与
  续采排查。

所有表在容器启动时通过 `CREATE TABLE IF NOT EXISTS` 幂等建表，新增列通过
`ALTER TABLE ... ADD COLUMN` 幂等迁移，旧版本数据库可直接升级。

---

## 7. 运行测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
# 37 passed
```

测试覆盖（`tests/`）：UID 纯函数性质、跨中心碰撞隔离、嵌套序列改写、
像素/传输语法保持、引用先到后解析与跨中心不解析、同字节重传复用、
异字节/非法文件整批回滚、20 份上限、非 Explicit VR 拒绝、8 路并发与
6 路并发重传收敛；以及补采专项：源清单分页与空游标结束、完整多页补采、
同字节复用、截断/断流/HTTP 500 故障暂停且零半页、异字节冲突暂停后复用、
清单 SOP 不符暂停、提交后中断按游标接续、8 路并发续采不重复建批、
崩溃残留 `running` 作业启动重排队、补采期间上传与查询不受影响。

---

## 8. 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DICOM_INGEST_DATA_DIR` | `/data` | SQLite 库与对象文件根目录（Compose 已挂卷） |
| `DICOM_INGEST_MAX_FILES` | `20` | 单批文件数上限 |
| `DICOM_INGEST_MAX_FILE_BYTES` | `52428800` | 单文件大小上限 |
| `DICOM_INGEST_UID_ROOT` | `2.25` | 新 UID 根（2.25 = UUID 派生 UID 的 ISO/ITU 保留根） |
| `DICOM_INGEST_BUSY_TIMEOUT_MS` | `30000` | SQLite 写锁等待超时 |
| `DICOM_INGEST_BACKFILL_POLL_INTERVAL_S` | `0.25` | worker 无作业时的轮询间隔 |
| `DICOM_INGEST_BACKFILL_HTTP_TIMEOUT_S` | `30` | 远端清单/对象请求超时 |
| `DICOM_INGEST_BACKFILL_MAX_PAGE_SIZE` | `20` | 接受的远端清单单页上限 |
| `SOURCE_SERVICE_DATA_DIR` | `/source-data` | 本地影像源的研究目录与故障文件根目录（仅源服务） |
