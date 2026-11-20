# restart-permit-api

存储集群值班员用的**分批重启许可 API**。FastAPI + SQLite，单容器即可运行。

核心规则：

- 服务启动时读取**固定的分片投票副本清单**（`manifest.json`）。
- 节点探活报告携带**采样时间、健康状态、启动标识**；每个节点以**采样时间最新**的一份为准。
- 值班员提交**申请号 + 拟重启的一批节点**，满足以下条件才批准整批：
  1. 本批每个节点最新探活均健康；
  2. 本批节点未被任何**未结束许可**占用；
  3. 扣除 *本批节点 + 未结束许可占用节点 + 健康未知/异常节点* 后，本批涉及的**每个分片**剩余可用投票副本均**过半**；否则拒绝并列出受影响分片。
- **并发申请**在写事务（`BEGIN IMMEDIATE`）中串行判定，守住多数票。
- **同号同批重发**返回原许可（HTTP 200）；同号不同批返回 409。
- 许可、探活记录与换机登记落盘 SQLite，**服务重启后保留**，并继续按当前来源编号受理。
- **结束许可**要求本批每个节点的最新探活：在获批后采集、健康、启动标识与获批时不同；
  换过主机的节点还要求最新探活**在换机登记之后采集**。

## 节点换机（来源编号）

更换存储节点的主机时，**分片仍沿用原节点名**；新主机必须凭值班员发放的
**探活来源编号（source_id）**上报，系统据此区分新旧主机的报告：

- 值班员凭**未结许可**调用 `POST /permits/{request_id}/replacements`，为批内节点登记换机，
  领取新的来源编号（形如 `src-1`，全局递增）。
- **同一许可下对同一节点重复登记只返回原编号**（HTTP 200，幂等）；不同许可再次登记会发放新编号。
- 登记生效后：
  - 查询与计票**只看带当前编号的报告**；该节点首份新报告到达前视为**健康未知**；
  - 未换过主机的节点照常上报（报告不得携带 `source_id`）；
  - 换过主机的节点漏填或填错编号时，**整批报告都不入库**（事务回滚，422）。
- 结单仍按原条件判定，换机节点的最新探活还须在**登记之后**采集
  （不满足时原因 `not_sampled_after_replacement`）。
- 登记、上报、发证、结单并发时，全部写路径共用同一串行写事务，**按实际生效顺序判定**。
- 服务沿用原库重启后，仍按当前编号受理；旧版数据库（无 `source_id` 列）启动时自动迁移。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

- SQLite 数据在命名卷 `permit-data`（容器内 `/data/permits.db`），重启不丢。
- 分片清单 `./manifest.json` 只读挂载进容器；修改后需 `docker compose restart` 生效。

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DB_PATH=./data/permits.db MANIFEST_PATH=./manifest.json \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pytest tests/ -q
```

## 分片清单格式

```json
{
  "shards": {
    "shard-a": ["n1", "n2", "n3"],
    "shard-b": ["n2", "n3", "n4"],
    "shard-c": ["n3", "n4", "n5"]
  }
}
```

每个分片列出全部投票副本节点；过半线 = `floor(票数/2)+1`（3 票需 2 可用）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 服务健康检查 |
| GET | `/manifest` | 查看分片清单与各分片过半线 |
| POST | `/liveness` | 批量上报探活（201；来源编号不符整批 422 拒收） |
| GET | `/liveness` | 全部节点当前生效的最新探活 + 当前来源编号 |
| GET | `/liveness/{node}` | 单节点最新探活（换机后首份新报告前 404） |
| POST | `/permits` | 申请许可（201 新批 / 200 幂等重发 / 409 拒绝） |
| GET | `/permits?status=open\|finished` | 许可列表 |
| GET | `/permits/{request_id}` | 单个许可 |
| POST | `/permits/{request_id}/finish` | 结束许可（200 / 409 条件不满足） |
| POST | `/permits/{request_id}/replacements` | 登记换机领取来源编号（201 / 200 幂等） |
| GET | `/permits/{request_id}/replacements` | 查看该许可下的换机登记 |

## 换机端到端演示（Docker Compose）

```bash
docker compose up --build -d
./examples/host-replacement-demo.sh
```

脚本自动演示：全量上报 → 申请许可 → 登记换机领取编号（重复登记幂等）→
旧主机报告（漏填/错填编号、混合批次）全部拒收 → 首份新报告前健康未知 →
新主机带当前编号上报 → 凭登记后采集的新探活结单。可对同一服务重复执行
（每次使用唯一申请号）。

## 调用示例

### 1. 上报探活

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports": [
      {"node_id":"n1","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n2","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n3","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n4","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n5","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"}
    ]}'
# => {"accepted":5}
```

### 2. 申请许可

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","nodes":["n1"],"status":"open",
#  "approved_at":"...","finished_at":null,
#  "boot_ids_at_approval":{"n1":"boot-1"}}
```

同号同批重发，返回原许可（200，`approved_at` 不变）；同号不同批返回 409。

### 3. 多数票不足被拒（列出受影响分片）

`req-001` 已占用 n1 时再申请 n2（同属 shard-a，3 票最多扣 1 票）：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n2"]}'
# 409 =>
# {"detail":{"ok":false,"error":"permit_rejected",
#   "affected_shards":[{"shard":"shard-a","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n3"],
#     "excluded":{"n1":"occupied_by:req-001","n2":"in_batch"}}]}}
```

其它拒绝类别：`health_unknown_nodes`（健康未知）、`unhealthy_nodes`（异常）、
`occupied_nodes`（被未结束许可占用）。清单外节点一律 422。

### 4. 节点重启后上报新探活，结束许可

```bash
# 节点重启完成：采样时间晚于获批时间、健康、启动标识已变
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-19T10:20:00Z",
                "healthy":true,"boot_id":"boot-2"}]}'

curl -X POST http://localhost:8000/permits/req-001/finish
# 200 => {"request_id":"req-001",...,"status":"finished","finished_at":"..."}
```

结束条件不满足时 409 并给出逐节点原因，例如：

```json
{"detail":{"error":"finish_conditions_not_met",
           "nodes":{"n1":"boot_id_unchanged"}}}
```

可能原因：`no_liveness_report`、`not_sampled_after_approval`、`unhealthy`、`boot_id_unchanged`。
许可结束后节点被释放，可被后续申请占用。

### 5. 节点换机：登记、拒收旧主机报告、凭新探活结单

```bash
# 5.1 凭未结许可 req-001 为批内节点 n1 登记换机，领取来源编号
curl -X POST http://localhost:8000/permits/req-001/replacements \
  -H 'Content-Type: application/json' -d '{"nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","replacements":[
#   {"node_id":"n1","source_id":"src-1",
#    "registered_at":"...","request_id":"req-001"}]}

# 同许可同节点重复登记 -> 200，只返回原编号 src-1
curl -X POST http://localhost:8000/permits/req-001/replacements \
  -H 'Content-Type: application/json' -d '{"nodes":["n1"]}'

# 5.2 旧主机报告：漏填来源编号 -> 422，整批不入库
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-20T10:00:00Z",
                "healthy":true,"boot_id":"boot-old"}]}'
# 422 =>
# {"detail":{"error":"source_id_mismatch","problems":[
#   {"node_id":"n1","expected_source_id":"src-1","got_source_id":null}]}}

# 填错编号同样 422；混合批次中任一报告来源不符，整批回滚，其他节点也不入库
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-20T10:01:00Z",
                "healthy":true,"boot_id":"boot-old","source_id":"src-wrong"}]}'

# 5.3 首份新报告到达前：查询 404，节点列入 nodes_without_reports（健康未知）
curl http://localhost:8000/liveness/n1   # 404 no_liveness_report

# 5.4 新主机带当前编号上报（采样时间须晚于登记时间）
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-20T11:00:00Z",
                "healthy":true,"boot_id":"boot-2","source_id":"src-1"}]}'

# 5.5 结单：新探活在登记后采集、健康、启动标识已变
curl -X POST http://localhost:8000/permits/req-001/finish
```

未换机节点的报告**不带** `source_id`，行为与旧版完全一致；若擅自携带编号，
同样按来源不符 422 拒收。结单条件不满足时，换机节点可能返回的原因
`not_sampled_after_replacement`（最新有效探活未在登记之后采集）。

## 设计说明

- **幂等**：`request_id` 是许可表主键。节点集合规范化（去重排序）后比较，
  同号同批重放直接返回原许可；同号不同批 409。被拒绝的申请不落库，整改后可用同号重试。
  换机登记以 `(request_id, node_id)` 唯一约束兜底，重复登记返回原来源编号。
- **并发**：所有写路径（申请、登记、上报、结单）走单连接 + 可重入锁 +
  `BEGIN IMMEDIATE` 事务，串行提交，按实际生效顺序判定，不会超批、不会错收报告。
- **持久化**：许可、全部探活报告、换机登记存 SQLite（WAL 模式）。
  最新探活实时计算：换机节点只匹配其当前 `source_id`，未换机节点只匹配
  `source_id IS NULL`；服务重启后状态完整恢复，继续按当前编号受理。
  旧库启动时自动 `ALTER TABLE` 补齐来源编号列，历史报告视为无编号（未换机）。
- **结束判定**：批准时快照各节点 `boot_id`；结束时要求最新有效报告
  `sampled_at > approved_at` 且健康且 `boot_id` 不同；换机节点再要求
  `sampled_at > 登记时间`。

## 已知限制 / 未完成项

- 单 worker 运行（SQLite 单写者）；如需多副本需换外部数据库。
- 未做认证鉴权，假定部署在受信内网；如需可前置网关或加 token。
- 探活无"过期"概念：只要最新一份健康即视为健康，不判断采样时间新旧。
- 结束许可比较的是节点上报的采样时钟与服务端获批时钟，依赖各节点时钟基本同步（NTP）。
- 许可无过期/自动回收机制；值班员需显式 finish。
