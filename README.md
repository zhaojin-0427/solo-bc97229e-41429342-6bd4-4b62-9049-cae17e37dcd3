# Multi-Center DICOM Ingest API

多中心 DICOM 入库平台。解决的核心问题：**设备在不同中心复用同一个 DICOM UID**
时，分割/引用结果可能错误地指向别家中心的影像。本平台对每个来源中心独立、确定
性地重映射所有实例标识，使跨中心数据在物理上无法串联。

技术栈：**FastAPI + pydicom + SQLite（WAL 模式）**，单容器即可运行，交付
Docker Compose 环境与交互式 API 文档（Swagger UI）。

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

---

## 2. 目录结构

```
app/
  config.py        环境变量配置（数据目录、UID 根、批量上限等）
  uid.py           确定性 UID 重映射（uuid5 + 2.25 根）
  dicom_io.py      DICOM 解析/校验、递归 UID 改写、序列化后回读校验
  db.py            SQLite schema、WAL 连接、即时写事务
  service.py       批次原子入库、重传复用、字节冲突、引用解析
  main.py          FastAPI 路由与响应模型
scripts/
  generate_samples.py  生成合成样例（跨中心碰撞 / 引用先到 / 非法文件）
  demo.sh              对运行中的服务执行完整 HTTP 演示
tests/             18 项 pytest（含并发、原子性、嵌套改写、重启稳定性）
samples/generated/ 合成 DICOM 样例（构建时生成）
Dockerfile, docker-compose.yml, docker/entrypoint.sh
```

---

## 3. 快速开始（Docker Compose）

```bash
# 1. 生成合成样例（需要本机有 pydicom；也可直接用仓库内已生成的文件）
python3 scripts/generate_samples.py

# 2. 构建并启动
docker compose up --build -d

# 3. 健康检查
curl http://localhost:8000/healthz
# {"status":"ok"}

# 4. 打开交互式 API 文档
#    浏览器访问 http://localhost:8000/docs
```

在 Swagger UI 中演示上传：

1. 展开 `POST /api/v1/batches` → **Try it out**
2. `center_id` 填 `center-a`
3. `files` 选择 `samples/generated/cross_center_uid_collision/center-a/image_001.dcm`
4. **Execute**，响应 201，返回每个文件的原/新 UID 映射
5. 用 `GET /api/v1/batches/{batch_id}` 查询批次，用
   `GET /api/v1/instances/{id}/download` 下载改写后的 DICOM

### 一键脚本演示（覆盖全部需求场景）

```bash
BASE_URL=http://localhost:8000 bash scripts/demo.sh
```

脚本依次演示：跨中心 UID 碰撞、引用先到、跨中心不解析、同中心影像到达后解析、
同字节重传复用、异字节整批 409、批次查询与下载校验像素不变、非法文件整批 422。

### 不用 Docker 的本地运行

```bash
pip install -r requirements.txt
python3 scripts/generate_samples.py
DICOM_INGEST_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 4. HTTP API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | multipart 表单：`center_id` + `files`（1–20），原子入库 |
| GET | `/api/v1/batches` | 批次列表，可按 `center_id` 过滤 |
| GET | `/api/v1/batches/{batch_id}` | 批次详情 + 每个文件的原/新 UID 映射 |
| GET | `/api/v1/instances` | 实例检索（`center_id` / `original_sop_uid` / `new_sop_uid`） |
| GET | `/api/v1/instances/{id}/download` | 下载改写后的 DICOM（`application/dicom`） |
| GET | `/api/v1/references/pending` | 某中心的引用解析状态（`resolved=true/false`） |
| GET | `/healthz` | 存活探针 |

错误语义：`422` = 文件/参数不合法（含非法 DICOM、传输语法不符、超 20 份）；
`409` = 与既有状态冲突（异字节同 UID、库级唯一约束冲突）；`404` = 批次/实例不存在。

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

---

## 6. 数据模型要点

- `uid_mappings`：每个 `(center, 原UID)` 一行，`present` 标识携带该 SOP 的实体
  是否已到达，支撑引用先到。
- `instances`：每个入库对象一行，含原/新三套 UID、SHA-256、存储相对路径，
  `(center, original_sop_uid, sha256)` 与 `new_sop_uid` 均唯一。
- `instance_references`：引用关系与解析状态，按中心隔离。
- `batches`：仅记录成功提交的批次；被拒绝的批次不留任何记录或对象。

所有表在容器启动时通过 `CREATE TABLE IF NOT EXISTS` 幂等建表。

---

## 7. 运行测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
# 18 passed
```

测试覆盖：UID 纯函数性质、跨中心碰撞隔离、嵌套序列改写、像素/传输语法保持、
引用先到后解析与跨中心不解析、同字节重传复用、异字节/非法文件整批回滚、
20 份上限、非 Explicit VR 拒绝、8 路并发与 6 路并发重传收敛。

---

## 8. 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DICOM_INGEST_DATA_DIR` | `/data` | SQLite 库与对象文件根目录（Compose 已挂卷） |
| `DICOM_INGEST_MAX_FILES` | `20` | 单批文件数上限 |
| `DICOM_INGEST_MAX_FILE_BYTES` | `52428800` | 单文件大小上限 |
| `DICOM_INGEST_UID_ROOT` | `2.25` | 新 UID 根（2.25 = UUID 派生 UID 的 ISO/ITU 保留根） |
| `DICOM_INGEST_BUSY_TIMEOUT_MS` | `30000` | SQLite 写锁等待超时 |
