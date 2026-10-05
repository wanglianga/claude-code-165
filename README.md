# 连锁烘焙门店临期食品捐赠预约冷链凭证服务

门店每天闭店前上报可捐赠的临期烘焙食品，公益机构按**服务半径、日接收能力、冷藏车辆、受助人需求、时效可达、机构履约可靠度**六类规则预约批次；门店确认后生成取货码、逐箱封签与待交接清单。取货时门店员工扫码核对**箱号、温度、封签、数量与接收人身份**，双方签收后机构上传配送计划、登记最终发放对象类型。每个批次生成一条**来源 → 温控 → 交接 → 配送 → 发放的 SHA-256 哈希链凭证**，支撑监管抽查与品牌风险控制；捐赠公示读模型由事件流投影维护，并可随时从权威表全量重建对账。

## 技术栈

- **Go 1.21**（本地 Go 1.19 亦可编译，未使用 1.20+ 语法特性）：HTTP API（chi）、领域服务、规则引擎、凭证哈希链、定时扫描器
- **PostgreSQL 15**：领域权威表、状态机约束、`SELECT … FOR UPDATE SKIP LOCKED`、**部分唯一索引防重复预约**
- **Redis 7**：预约热点互斥锁（SET NX PX）、实时公益/损耗统计
- **Kafka 3.7（KRaft，无 ZooKeeper）**：事务性 outbox 中继 + 消费组幂等投影；设置 `KAFKA_ENABLED=false` 时投影器直接消费 outbox，无 Kafka 也能完整运行
- OpenAPI 3.0 + 内嵌 Swagger UI

## 一键启动（Docker Compose）

```bash
docker compose up --build
# 服务: http://localhost:8080
# Swagger UI: http://localhost:8080/docs
# OpenAPI: http://localhost:8080/openapi.yaml
```

Compose 启动 PostgreSQL、Redis、Kafka（KRaft）与应用；应用自动执行数据库迁移并创建演示账号。

跑一遍完整业务演示（需要 `curl` 和 `jq`）：

```bash
./scripts/demo.sh
```

## 测试账号（密码统一 `Test@1234`）

| 账号 | 角色 | 归属 |
|---|---|---|
| `store1` | 门店员工 | 麦香里·中山公园店 SH001 |
| `store2` | 门店员工 | 麦香里·徐家汇店 SH002 |
| `charity1` | 公益机构 | 暖阳社区公益服务中心（8km、40箱/日、2台冷藏车，需求：面包/糕点/乳制甜品/三明治） |
| `charity2` | 公益机构 | 晚晴养老互助社（5km、20箱/日、1台冷藏车，需求：面包/糕点） |
| `charity3` | 公益机构 | 远郊爱心接力站（3km、10箱/日、**无冷藏车**，仅需求乳制甜品——用于演示规则拦截） |
| `regulator` | 监管检查员 | 可抽查凭证链、查看异常与抽查记录 |
| `admin` | 平台管理员 | 手动对账重建、触发扫描、可代任意角色操作 |

## 本地开发（不使用 Docker）

需要 Go 1.19+、PostgreSQL、Redis；Kafka 可选。

```bash
# 1. 数据库
createdb bakery
# 2. 依赖（可选）
#    Redis: localhost:6379
#    Kafka(KRaft): localhost:9092  —— 不需要时设置 KAFKA_ENABLED=false
# 3. 运行
export DATABASE_URL="postgres://$USER@localhost:5432/bakery?sslmode=disable"
export KAFKA_ENABLED=false     # 无 Kafka 时
go run ./cmd/server
```

环境变量：`HTTP_ADDR`（默认 `:8080`）、`DATABASE_URL`、`REDIS_ADDR`、`REDIS_DB`、
`KAFKA_ENABLED`、`KAFKA_BROKERS`、`KAFKA_TOPIC`、`TOKEN_SECRET`。

## 验证方式

### 自动化测试

```bash
# 需要本地 PostgreSQL（测试库 bakery_test）与 Redis
createdb bakery_test
go test ./internal/rules/                       # 规则引擎单元测试
go test ./test/                                 # 端到端（投影器直接消费 outbox）
TEST_KAFKA=true go test ./test/                 # 端到端（事件真实经过 Kafka）
```

端到端套件（`test/e2e_test.go`，httptest 全链路）实际覆盖：

1. **正常全流程**：上报（面包+冷藏蛋糕 6 箱）→ 无冷藏车机构被规则拦截 → **并发 2 次预约恰好 1 成功 1 冲突（409）**，串行再约同样 409 → 门店确认（取货码+封签）→ 到达 → 错误取货码被拒 → 未知箱号错扫登记 → 6 箱扫码全过 → 完成核对 → 门店先签、机构后签 → 运输温度 → 配送计划（2 个受助点）→ 6 箱逐箱发放至「困难家庭/住养老人」→ 公示投影为 `donated` 且五证齐全 → **监管抽查 pass（哈希链重算通过、五证完整、公示一致）**；
2. **临时售罄**：已预约批次标记售罄 → 预约自动撤销、公示为 `wasted`；
3. **爽约扫描**：未到计划时间不误判；超过宽限期未到达 → 预约 `no_show`、机构爽约计数+可靠度下降、批次 `released` → 门店重新开放后可被再次预约；
4. **过敏原缺失 + 变质反馈**：4 个未声明过敏原的面包箱扫码被拒收（异常留痕后随拒收关闭、留店不出库），2 个冷链蛋糕箱正常出库签收；受助点反馈变质 → 批次 `quarantined`、公示 `spoiled`、监管抽查 `defect`；
5. **对账重建**：`/admin/reconcile` 从权威表全量重建公示与 Redis 统计。

### 手工验证（Swagger 或 curl）

`./scripts/demo.sh` 会顺序打印每一步结果，最后一步输出监管抽查结论：
`chain_ok / five_vouchers_complete / ledger_consistent / result=pass` 以及链上凭证类型
`["source","temperature","handover","delivery","temperature","distribution",...]`。

## 领域流程与异常处理

批次状态机：

```
open ──预约──▶ reserved ──门店确认──▶ confirmed ──车辆到达──▶ in_handover
  ▲                │                      │                    │机构签收
  │门店拒绝        │门店拒绝              │                    ▼
  └────────────────┴──────────────▶ open │              in_transit ──▶ delivered ──▶ distributed
released ──门店重新开放──▶ open            │                    │
异常终态：sold_out（临时售罄）/ expired（超可捐赠时限，定时扫描）/ cancelled /
          quarantined（受助点变质反馈）
```

| 业务情况 | 系统行为 |
|---|---|
| 门店临时售罄 | `sold-out` 关闭批次，有效预约自动撤销并通知（事件），公示记 `wasted`；已确认批次禁止直接售罄 |
| 超过可捐赠时限 | 服务按 `保质期 − 安全缓冲（冷藏4h/常温2h）` 计算时限；定时扫描（30s）把 open/released/reserved 批次置 `expired`，待确认预约自动失效 |
| 冷链车迟到 | 到达时间晚于计划时间 15 分钟以上：记 `cold_truck_late`、机构迟到计数，可靠度重算 |
| 公益机构爽约 | confirmed 后超过计划取货时间 20 分钟未到达：预约 `no_show`、爽约计数加权 2；批次有时限则 `released` 可重新开放，否则 `expired` |
| 箱号错扫 | 箱号不存在 / 不属于本批次：拒绝并登记 `wrong_scan` 异常，错扫不阻断本批其余箱交接 |
| 数量/封签/温度不符 | 该箱判 `rejected` 留店、不进入公益流；冷藏箱无温度或超过品类温度上限另记 `temp_breach` |
| 过敏原信息缺失 | 上报时即记非阻断 `allergen_missing`；对应箱扫码时**拒收并阻断出库**，避免不明过敏原食品流入受助人群 |
| 受助点反馈变质 | 批次/箱 `quarantined` 封存，记 `recipient_spoiled` 阻断异常与异常凭证，公示 `spoiled`，抽查判 defect |
| 监管抽查 | 逐节重算 SHA-256 哈希链；按批次阶段校验五证完整性；比对公示读模型与权威表；抽查结论持久化 |

## 防重复预约与一致性

1. **Redis 锁**：`SET NX PX` 对同一批次的预约请求串行化（锁竞争返回 429，由调用方重试）；
2. **PostgreSQL 行锁**：预约事务内 `SELECT … FOR UPDATE` 锁定批次；
3. **部分唯一索引（硬约束）**：
   ```sql
   CREATE UNIQUE INDEX uq_active_reservation ON reservations(batch_id)
     WHERE status IN ('requested','confirmed');
   ```
   即使前两层失效，数据库也保证同一批次同时只有一个有效预约。
4 事件走**事务性 outbox**，与业务数据同事务提交；relay 至少一次投递，消费端 `processed_events` 幂等去重。公示行每次都从权威表重算（不依赖事件字段），`/admin/reconcile` 可全量重建，Redis 统计以状态迁移为增量并可被对账覆盖。

## 凭证哈希链

每个批次一条链：`chain_hash = sha256(prev_hash | payload_hash)`，创世 `prev_hash=GENESIS`。
凭证类型：`source`（品类/数量/生产时间/保质期/过敏原/冷藏/包装/位置/时限）、
`temperature`（交接/运输/受助点逐次测温，含限值与是否合规）、
`handover`（接收人脱敏身份、取货码核验、逐箱封签/数量/温度核对、双方签收时间）、
`delivery`（车牌/司机/是否冷藏/受助点与箱体分配）、
`distribution`（逐箱最终发放对象类型与受助点），异常另附 `exception` 凭证。
哈希对内容先做 JSON 归一化（map 键排序、时间截断到与 PostgreSQL timestamptz 一致的微秒精度），保证抽查时从数据库读回内容可复现哈希。

## API 概览

见 `openapi.yaml`（Swagger UI：`/docs`）。主要端点：

- 认证：`POST /api/v1/auth/login`
- 门店：`POST /batches/report`、`POST /batches/{id}/decision`、`/verify-receiver`、`/scan-box`、`/complete-check`、`/sign`、`/sold-out`、`/cancel`、`/release`
- 机构：`GET /charities/me/open-batches`（含规则预检）、`POST /reservations`、`/arrive`、`POST /batches/{id}/temperatures`、`POST /delivery-plans`、`POST /distributions`、`/spoiled-feedback`
- 公示/查询：`GET /ledger`、`GET /stats`、`GET /batches/{id}`、`GET /batches/{id}/vouchers`、`GET /exceptions`
- 监管/平台：`POST /batches/{id}/inspections`、`GET /inspections`、`POST /admin/reconcile`、`POST /admin/sweep`

## 目录结构

```
cmd/server/            程序入口（迁移/种子/relay/投影器/扫描器/HTTP 启动）
internal/config/       环境配置与食安规则参数
internal/database/     连接池、嵌入式 SQL 迁移、账号种子（migrations/）
internal/auth/         bcrypt + HMAC 签名令牌、角色 claims
internal/lock/         Redis 互斥锁（故障降级为不加锁，由 DB 唯一约束兜底）
internal/geo/          Haversine 距离
internal/rules/        预约资格规则引擎（公益效率 + 食品安全）
internal/events/       事件类型、事务 outbox、Kafka relay
internal/projector/    事件消费者：公示读模型 + Redis 统计，全量对账重建
internal/voucher/      SHA-256 哈希链
internal/service/      领域服务（批次/预约/交接/温控/配送/发放/扫描/抽查/查询）
internal/api/          HTTP 处理器、角色鉴权、OpenAPI 与 Swagger UI
test/                  端到端集成测试
scripts/demo.sh        全流程演示脚本
migrations/            SQL 迁移副本（便于审阅；实际由 internal/database 内嵌执行）
```
