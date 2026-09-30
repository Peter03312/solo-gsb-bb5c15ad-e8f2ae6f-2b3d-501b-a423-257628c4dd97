# 批次追溯服务（batch-trace）

面向小型食品加工厂的**本地批次追溯服务**：登记原料批次、记录生产投料与产出、
查询批次余量与上下游谱系，并在发现疑似污染批次时找出并隔离全部后代产出；
生产单录入错误时可整单撤销，在同一事务内返还各来源原投料量。

- 语言/框架：Python 3.12 + FastAPI + SQLAlchemy 2
- 关系数据库：PostgreSQL 16（Docker Compose 一键启动）；无数据库时自动回退本地 SQLite 文件库
- 计量：`NUMERIC(18,4)` 定点数，数量必须为**正数**，同一笔单据内**单位一致**
- 部署：Dockerfile 构建镜像，`docker compose up` 安装依赖、建库建表并启动

---

## 一、快速启动

### 方式 A：Docker Compose（推荐，含 PostgreSQL）

```bash
docker compose up --build -d
```

启动后：

- API 服务地址：`http://localhost:8080`
- 健康检查：<http://localhost:8080/health>
- 交互式接口文档（Swagger UI）：<http://localhost:8080/docs>

API 容器会等待 PostgreSQL 健康后自动**建库建表**（`app/entrypoint.py` 调用 `CREATE TABLE IF NOT EXISTS ...`），
数据持久化在 compose 卷 `trace_pgdata` 中。

停止与清理：

```bash
docker compose down          # 停止，保留数据
docker compose down -v       # 停止并删除数据库卷
```

### 方式 B：本地直接运行（自动使用 SQLite）

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m app.entrypoint
```

不设置 `DATABASE_URL` 时会在项目根目录创建 `trace.db`，服务同样监听 `http://0.0.0.0:8080`。

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest
```

---

## 二、配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | 见下 | SQLAlchemy 数据库连接串。compose 中为 `postgresql+psycopg2://trace:trace@db:5432/trace`；未设置时回退 `sqlite:///./trace.db` |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `db` / `5432` / `trace` / `trace` / `trace` | 未设置 `DATABASE_URL` 时用于拼装 PostgreSQL 连接串 |
| `HOST` | `0.0.0.0` | HTTP 监听地址 |
| `PORT` | `8080` | HTTP 监听端口 |
| `DB_POOL_SIZE` | `5` | PostgreSQL 连接池大小 |

compose 的端口映射：宿主机 `8080` → 容器 `8080`，可在 `docker-compose.yml` 的
`ports` 处修改宿主机端口。

---

## 三、HTTP 接口

所有写操作（`POST`）都必须携带请求头 `Idempotency-Key`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/raw-batches` | 登记一个原料批次及初始数量 |
| POST | `/productions` | 提交一次生产：多批次投入 → 一个产出批次 |
| POST | `/productions/{output_no}/cancellation` | 撤销一张录错的生产单，同事务返还各来源原投料量 |
| GET | `/productions/{output_no}` | 按产出编号查询生产原单据与撤销状态 |
| GET | `/batches/{batch_no}` | 查询批次余量与状态 |
| GET | `/batches/{batch_no}/lineage` | 查询上下游谱系（递归，仅含未撤销投料边） |
| POST | `/contamination` | 标记疑似污染批次，返回并隔离全部后代 |
| POST | `/reviews` | 质检复核确认误报：撤销疑似标记并重算隔离 |

### 业务规则

1. 数量一律为正数，使用定点数计算；同一生产单内所有批次单位必须一致。
2. 产出量 **不得超过** 投入总量（允许加工损耗，即产出 < 投入）。
3. 批次编号全局唯一、**不得复用**（原料批次与产出批次共用编号空间）。
4. 来源不存在、用量无效（非正数）、单位不一致或余量不足时，**整笔拒绝，不改变任何余量**。
5. 污染规则：标记某批次疑似污染后，其全部直接/间接产出（混料后代）**整体隔离**；
   隔离批次（含疑似批次本身）**不得再投料**，提交生产时整笔拒绝。
6. 复核规则：质检复核确认误报时，`POST /reviews` 提交**当前疑似批次编号**与
   **复核说明**（非空白），撤销其疑似标记并重算该批次及全部后代的隔离状态——
   自身或任一祖先仍疑似则继续隔离，否则解除。混料后代不会因一个来源解除就忽略
   另一疑似来源；编号不存在报 `SOURCE_NOT_FOUND`(404)，批次当前非疑似（从未标记
   或已撤销）报 `BATCH_NOT_SUSPECT`(409)，二者均不改变任何状态。批次可经历
   “标记 → 复核撤销 → 再次标记 → 再次复核”。
7. 幂等：同一 `Idempotency-Key` + 同一请求体（撤销接口还包含路径中的产出编号）重试，返回首次的状态码与响应体、不重复扣减、不重复撤销、不重复返还；
   同一 key + 不同请求体返回 `409 IDEMPOTENCY_CONFLICT`。
8. 并发：所有写事务（投料、污染标记、复核、**撤销**）在事务开始即取同一把串行化锁
   （PostgreSQL `LOCK TABLE batches IN SHARE ROW EXCLUSIVE`，写事务之间互斥、与普通
   SELECT 不冲突；SQLite 以 `BEGIN IMMEDIATE` 串行化写事务），随后按编号升序加行锁。
   并发提交**不会超用**同一批次余量；撤销与投料、污染标记、复核任意并发，结果都等价于
   四者的某一串行顺序——隔离批次绝不会被投料，撤销绝不会与并发投料互相超用或误放行。
9. 生产单撤销：生产单录入错误后，操作员可凭**产出批次编号**与**非空白撤销原因**
   `POST /productions/{output_no}/cancellation` 撤销该单。仅允许撤销**同时**满足以下条件
   的产出，任一不符明确拒绝且**不改变任何库存**：
   - 从未被后续**有效**生产投料（引用它的生产单若也已撤销，则不算“后续投料”）；
   - 产出批次**当前非疑似**；
   - 产出批次**无质检复核记录**；
   - 该生产单**尚未撤销**（编号不存在 404、批次不是生产产出 409，见错误码表）。

   撤销在**同一事务内**把各来源的**原投料量**加回来源余量——即使来源批次已隔离
   （疑似/隔离不阻止返还，返还只是冲销本单的错误扣减，不改变来源隔离状态）。
   撤销后：产出余量置 0 且不得再投料；产出编号、原单据（投入明细、产出量）与撤销原因
   保留可查（`GET /productions/{output_no}`）；该单投料边**不再进入有效谱系查询**，
   也**不参与疑似隔离传播/祖先隔离谓词**；产出批次的隔离标记随失效边一并清除。

### 错误响应格式

```json
{ "error": { "code": "INSUFFICIENT_BALANCE", "message": "批次 'RM-1' 余量不足：请求 200，剩余 100", "details": { } } }
```

常见错误码：`SOURCE_NOT_FOUND`(404)、`BATCH_NO_REUSED`(409)、`INSUFFICIENT_BALANCE`(409)、
`UNIT_MISMATCH`(422)、`OUTPUT_EXCEEDS_INPUT`(422)、`VALIDATION_ERROR`(422)、
`BATCH_QUARANTINED`(409)、`BATCH_NOT_SUSPECT`(409)、`IDEMPOTENCY_CONFLICT`(409)，
以及生产单撤销相关：`BATCH_CANCELLED`(409，已撤销产出被再次投料)、
`NOT_PRODUCTION_OUTPUT`(409，编号不是生产产出)、`PRODUCTION_HAS_DOWNSTREAM`(409，已被后续有效生产投料)、
`BATCH_SUSPECT`(409，产出当前疑似)、`PRODUCTION_HAS_REVIEW`(409，产出有复核记录)、
`PRODUCTION_ALREADY_CANCELLED`(409，重复撤销)。

---

## 四、HTTP 调用样例

### 1. 登记原料批次

```bash
curl -X POST http://localhost:8080/raw-batches \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: raw-rm-a-001' \
  -d '{"batch_no":"RM-A","unit":"kg","quantity":"100","note":"高筋面粉"}'
```

```json
{"batch_no":"RM-A","unit":"kg","initial_qty":100.0,"remaining_qty":100.0,
 "is_suspect":false,"is_quarantined":false,"note":"高筋面粉"}
```

### 2. 提交一次混料生产（多批次投入 → 一个产出）

```bash
curl -X POST http://localhost:8080/productions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: prod-mix1-001' \
  -d '{
        "output_batch_no": "MIX-1",
        "unit": "kg",
        "output_quantity": 90,
        "inputs": [
          {"batch_no": "RM-A", "quantity": 40},
          {"batch_no": "RM-B", "quantity": 55}
        ]
      }'
```

### 3. 安全重试（同一 key + 同一内容）

```bash
# 与上一条完全相同的请求：返回原结果，余量只扣一次
curl -X POST http://localhost:8080/productions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: prod-mix1-001' \
  -d '{ ...同上... }'

# 同一 key 换不同内容：
# 409 {"error":{"code":"IDEMPOTENCY_CONFLICT", ...}}
```

### 4. 查询批次余量

```bash
curl http://localhost:8080/batches/RM-A
# {"batch_no":"RM-A","unit":"kg","initial_qty":100.0,"remaining_qty":60.0, ...}
```

### 5. 查询上下游谱系

```bash
curl http://localhost:8080/batches/RM-A/lineage
```

```json
{
  "batch_no": "RM-A",
  "upstream": [],
  "downstream": [
    {"batch_no": "MIX-1", "depth": 1, "remaining_qty": 40.0,
     "unit": "kg", "is_suspect": false, "is_quarantined": false},
    {"batch_no": "FG-1",  "depth": 2, "remaining_qty": 50.0,
     "unit": "kg", "is_suspect": false, "is_quarantined": false}
  ],
  "edges": [
    {"from": "RM-A", "to": "MIX-1"},
    {"from": "MIX-1", "to": "FG-1"}
  ]
}
```

`depth` 表示与查询批次相隔的生产层数；`edges` 给出当前谱系子图中的“原料 → 产出”有向边。

### 6. 标记疑似污染并找出全部后代

```bash
curl -X POST http://localhost:8080/contamination \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: cont-20260930-01' \
  -d '{"batch_no":"RM-A","note":"抽检农残超标"}'
```

```json
{
  "source": "RM-A",
  "is_suspect": true,
  "affected_batch_nos": ["MIX-1", "FG-1"],
  "newly_quarantined": ["MIX-1", "FG-1"]
}
```

`MIX-1`（混料时混入 RM-A）与 `FG-1`（间接后代）被**整体隔离**；与 RM-A 无关的批次不受影响。
此后再用隔离批次投料会被拒绝：

```bash
curl -X POST http://localhost:8080/productions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: prod-fg2-001' \
  -d '{"output_batch_no":"FG-2","unit":"kg","output_quantity":5,
       "inputs":[{"batch_no":"MIX-1","quantity":5}]}'
# 409 {"error":{"code":"BATCH_QUARANTINED","message":"批次 'MIX-1' 已隔离，隔离批次不得再投料"}}
```

### 7. 质检复核：确认误报，撤销疑似标记并重算隔离

沿用上例：`RM-A` 已被标记疑似，混料后代 `MIX-1 = RM-A + RM-B`、`MIX-1 -> FG-1`
均被隔离。质检复检确认 RM-A 为误报：

```bash
curl -X POST http://localhost:8080/reviews \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: review-rm-a-20260930-01' \
  -d '{"batch_no":"RM-A","review_note":"留样复测未检出农残，确认误报"}'
```

```json
{
  "reviewed_batch_no": "RM-A",
  "is_suspect": false,
  "review_note": "留样复测未检出农残，确认误报",
  "affected_batch_nos": ["MIX-1", "FG-1"],
  "released_batch_nos": ["FG-1", "MIX-1", "RM-A"],
  "still_quarantined": []
}
```

- `affected_batch_nos`：本次重算涉及的全部后代；
- `released_batch_nos`：本次**解除隔离**的批次（复核批次自身 + 不再有任何疑似
  祖先的后代）；`still_quarantined`：重算范围内仍须隔离的批次。
- 此后 `GET /batches/{no}` 与 `/batches/{no}/lineage` 同步反映 `is_suspect=false`、
  `is_quarantined=false`；解除隔离的批次可重新投料。

混料来源不会被“连带误放”：若 RM-B 当时也处于疑似状态，撤销 RM-A 只会放行
RM-A 自身，`MIX-1`/`FG-1` 因仍有疑似祖先 RM-B 而继续隔离：

```json
{ "released_batch_nos": ["RM-A"], "still_quarantined": ["FG-1", "MIX-1"] }
```

非法复核明确报错且不改变状态：

```bash
# 编号不存在
curl -i -X POST http://localhost:8080/reviews \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: rv-x' \
  -d '{"batch_no":"GHOST","review_note":"x"}'
# 404 SOURCE_NOT_FOUND

# 已撤销 / 从未疑似
curl -i -X POST http://localhost:8080/reviews \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: rv-a2' \
  -d '{"batch_no":"RM-A","review_note":"再次撤销"}'
# 409 BATCH_NOT_SUSPECT
```

### 8. 撤销录错的生产单（返还投料、边退出有效谱系）

沿用上例：`RM-A`(100kg)、`RM-B`(50kg) 曾以 40/20 投入产出 `MIX-1`(55kg)，
操作员发现产出量录入错误，凭产出编号与撤销原因整单撤销：

```bash
curl -X POST http://localhost:8080/productions/MIX-1/cancellation \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: cancel-mix1-20260930-01' \
  -d '{"reason":"操作员录入错误，产出量应为 50kg，整单撤销"}'
```

```json
{
  "output_batch_no": "MIX-1",
  "status": "cancelled",
  "reason": "操作员录入错误，产出量应为 50kg，整单撤销",
  "cancelled_at": "2026-09-30T15:23:59.857307",
  "returns": [
    {"source_batch_no": "RM-A", "returned_quantity": 40.0, "remaining_after": 100.0},
    {"source_batch_no": "RM-B", "returned_quantity": 20.0, "remaining_after": 50.0}
  ]
}
```

- 各来源在**同一事务**内返还原投料量（来源已隔离也照常返还，隔离状态不因此解除）；
- 产出批次保留但余量置 0、`is_cancelled=true`，再用它投料返回 `409 BATCH_CANCELLED`；
- 原单据与撤销原因保留可查：

```bash
curl http://localhost:8080/productions/MIX-1
# {"output_batch_no":"MIX-1","unit":"kg","output_quantity":55.0,"is_cancelled":true,
#  "inputs":[{"batch_no":"RM-A","quantity":40.0},{"batch_no":"RM-B","quantity":20.0}],
#  "cancellation":{"reason":"操作员录入错误，产出量应为 50kg，整单撤销",
#                  "cancelled_at":"2026-09-30T15:23:59.857307"}}
```

- 撤销的投料边退出有效谱系：`GET /batches/RM-A/lineage` 不再出现 `MIX-1` 及其边；
  此后标记 `RM-A` 疑似也不会沿该边传播到 `MIX-1`。
- 条件不符一律拒绝且不改库存（重复撤销用新的幂等键也一样拒绝）：

```bash
# 编号不存在
curl -i -X POST http://localhost:8080/productions/GHOST/cancellation \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: c-x' \
  -d '{"reason":"x"}'
# 404 SOURCE_NOT_FOUND

# 原料批次不是生产产出
# 409 NOT_PRODUCTION_OUTPUT

# 已被后续有效生产投料（details.consumers 给出消费方）
# 409 PRODUCTION_HAS_DOWNSTREAM

# 产出当前疑似 / 产出有复核记录 / 已撤销
# 409 BATCH_SUSPECT / 409 PRODUCTION_HAS_REVIEW / 409 PRODUCTION_ALREADY_CANCELLED

# 撤销原因为空白或缺失
# 422 VALIDATION_ERROR
```

- 幂等沿用既有约定：同键同体重放返回首次结果（**不会二次返还**），同键异体 `409 IDEMPOTENCY_CONFLICT`。
- 链式撤销：先撤销末端产出（返还中间品），中间品不再有有效下游后即可继续撤销。

### 9. 整笔拒绝示例（余量不足时不产生副作用）

```bash
# RM-A 当前余量 60，却请求消耗 200
curl -i -X POST http://localhost:8080/productions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: prod-bad-001' \
  -d '{"output_batch_no":"MIX-X","unit":"kg","output_quantity":200,
       "inputs":[{"batch_no":"RM-A","quantity":200}]}'
# HTTP/1.1 409 ... INSUFFICIENT_BALANCE；MIX-X 未创建，RM-A 余量仍为 60
```

---

## 五、项目结构

```
app/
  config.py       # 环境变量配置
  db.py           # 引擎/会话、SQLite 写锁与 PG 写串行化锁、建表与轻量列升级
  models.py       # batches / productions / production_inputs / batch_reviews /
                  # production_cancellations / idempotency_keys
  schemas.py      # Pydantic 请求响应模型（正数、定点数、非空白撤销原因校验）
  errors.py       # 业务错误码
  service.py      # 核心逻辑：登记、生产事务、撤销返还、幂等、递归谱系、污染传播、复核重算
  routes.py       # HTTP 路由
  main.py         # FastAPI 应用与异常处理
  entrypoint.py   # 容器入口：等待数据库 → 建表 → 启动
tests/            # 端到端 + 并发测试（54 个用例）
Dockerfile
docker-compose.yml
```

## 六、并发与一致性说明

- 全部写事务（生产投料、污染标记、质检复核、生产单撤销）开始时先取同一把串行化锁
  （PostgreSQL 为 `LOCK TABLE batches IN SHARE ROW EXCLUSIVE`：与普通 SELECT 兼容、
  与任意其它写事务互斥；SQLite 由 `BEGIN IMMEDIATE` 全库写锁覆盖），随后按编号升序
  逐个加行锁（`SELECT ... FOR UPDATE`）。统一的加锁顺序杜绝写事务间死锁。
- 因此：
  - 同一批次余量的并发扣减不会超用；撤销返还与并发投料不会互相超用；
  - 撤销 / 复核 / 污染标记 / 投料任意并发，结果都等价于某一串行顺序——撤销事务校验
    “产出未被后续投料”时不可能有并发投料插入，QC 闭包读到的谱系边也不会写到一半；
  - 隔离批次不可能被投料，已撤销产出也不可能被投料；撤销不会误放行隔离来源
    （返还只冲销错误扣减，不改变来源的疑似/隔离标志）。
- 幂等记录与业务数据在同一事务内落库，唯一约束 `(method, path, key)` 兜底并发首请求竞争。

> 注：新表 `batch_reviews`、`production_cancellations` 以及 `productions.is_cancelled`
> 列随启动/入口的建表与轻量列升级（`CREATE TABLE IF NOT EXISTS` + 按需 `ADD COLUMN`）
> 自动创建，既有数据库升级无需手工迁移；现有登记、生产、污染标记、批次/谱系查询接口的
> 请求与启动方式均保持不变，响应仅**追加**了 `is_cancelled` 字段（未撤销数据恒为
> `false`），`/batches/{no}/lineage` 的节点与边只包含未撤销投料，未撤销数据的谱系结果
> 与旧版本完全一致。
