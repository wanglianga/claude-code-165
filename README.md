# 临期食品捐赠预约冷链凭证服务

连锁烘焙门店临期食品捐赠的**预约 + 冷链 + 凭证**平台。门店闭店前上报可捐赠批次，公益机构按服务半径/接收能力/冷链能力预约，门店确认后扫码交接，机构签收后上传配送计划并分站点发放。每个批次沉淀 **来源 / 预约 / 确认 / 交接（温控+封签+身份）/ 配送 / 发放 / 异常 / 闭环** 的哈希链凭证，支撑监管抽查与品牌风险控制，避免临期食品在公益流向中失去食安责任。

## 技术栈

Go 1.19+ (Gin) · PostgreSQL 16 · Redis 7 · Kafka 3.7 (KRaft) · Docker Compose

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 |
|---|---|
| API | http://localhost:8080 （健康检查 `/healthz`） |
| Swagger UI | http://localhost:8081 （OpenAPI 3.0 文档与在线调试） |
| OpenAPI 源文件 | http://localhost:8080/openapi.yaml |
| 捐赠公示（公开） | http://localhost:8080/api/v1/publicity/donations |

启动后自动完成：建表迁移 → 演示数据（2 门店 / 2 机构 / 7 账号）→ 后台过期巡检任务 → Kafka outbox relay 与 consumer。

## 测试账号

| 账号 | 密码 | 角色 |
|---|---|---|
| `store1.manager` | `Store@123` | 中山公园店店长（确认预约） |
| `store1.staff` | `Store@123` | 中山公园店店员（上报/扫码交接） |
| `store2.manager` | `Store@123` | 张江店店长 |
| `charity1.staff` | `Charity@123` | 绿洲食物银行（有 2 辆冷藏车，半径 15km，日能力 200 箱） |
| `charity2.staff` | `Charity@123` | 晨光公益（无冷藏车，半径 8km，日能力 60 箱） |
| `regulator` | `Regulator@123` | 监管（全链路审计、抽查留痕、关闭异常） |
| `admin` | `Admin@123` | 平台管理员 |

登录：`POST /api/v1/auth/login` 获取 token，之后请求带 `Authorization: Bearer <token>`。

## 验证方式（本题已实际执行）

```bash
# 1) 单元测试：预约规则/状态机/凭证哈希链/温度校验
go test ./...

# 2) 端到端验证（需服务已启动）：79 项断言，覆盖主流程 + 全部异常场景
./scripts/verify.sh
```

本地验证环境（PostgreSQL 15 + Redis 7 + Kafka 3.9 KRaft）实测结论：

- `go build ./...`、`go vet ./...`、`gofmt` 全部通过；
- `go test ./...` 通过（domain 规则与凭证链 9 个用例）；
- `./scripts/verify.sh` **PASS=79 FAIL=0**，覆盖：
  - 主流程：上报(4 箱冷链批次) → 预约 → 店长确认 → 待交接清单 → 逐箱扫码 → 完成交接 → 配送计划(2 站点) → 发放闭环 → 公示；
  - 幂等与防重：Idempotency-Key 重放、同批次重复预约 409、重复扫码返回首扫记录、重复开始交接/重复发放幂等；
  - 规则：无冷藏车机构预约冷链批次 422、缺 Idempotency-Key 400、配送箱数不一致 422、接收人证件不符 422、未扫完箱不可完成交接 422；
  - 异常场景：箱号错扫(wrong_scan)、温度超标(temp_breach)、过敏原缺失阻断预约(allergen_missing)、门店临时售罄(sold_out→批次与预约取消)、超过可捐赠时限自动过期(expired)、冷链车迟到(truck_late)、机构爽约(no_show→批次过期)、受助点反馈变质(spoilage_report→3 箱标记 spoiled)、监管抽查(regulatory_check) 与异常关闭；
  - 凭证与审计：凭证链 `chain_valid=true`、监管审计接口返回全链路轨迹、公示发放箱数与交接一致；
  - Kafka 链路：27 条 outbox 事件全部发布，consumer 消费并投影公示（日志 `publicity projected batch=1`）。

## 本地开发（无 Docker）

依赖：PostgreSQL（建库 `donation`）、Redis、Kafka（可选，不配置则事件仅落库 outbox 表）。

```bash
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/donation?sslmode=disable'
export REDIS_ADDR='localhost:6379'
export KAFKA_BROKERS='localhost:9092'   # 可选
go run ./cmd/server
```

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `HTTP_ADDR` | `:8080` | 监听地址 |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/donation?sslmode=disable` | PG 连接 |
| `MIGRATIONS_DIR` | `migrations` | 迁移目录 |
| `REDIS_ADDR` | `localhost:6379` | Redis 地址 |
| `KAFKA_BROKERS` | 空（禁用） | 逗号分隔 |
| `KAFKA_TOPIC` | `donation-events` | 事件主题 |
| `SEED_DEMO` | `true` | 是否写入演示数据 |
| `TOKEN_TTL` | `24h` | 登录令牌有效期 |
| `EXPIRY_JOB_INTERVAL` | `30s` | 过期巡检周期 |
| `RESERVATION_TTL` | `30m` | 预约未确认自动释放时长 |

## 核心业务流与状态机

```
reported → reserved → confirmed → handover → handed_over → in_delivery → distributed
    │          │          │                     (售罄/爽约/超时) ↓
    └──────────┴──────────┴──→ cancelled / expired
```

1. **上报** `POST /api/v1/stores/{id}/batches`：记录品类、数量、生产时间、保质期、过敏原、是否冷藏、包装完整度、位置、最晚取走时间；自动生成箱号与来源凭证。包装破损直接拒绝；最晚取走时间不得超过保质期末。
2. **预约** `POST /api/v1/batches/{id}/reserve`（需 `Idempotency-Key` 头）：校验服务半径(haversine)、当日接收能力、冷链车辆与冷链容量、取货时限、过敏原已确认；**大批量释放规则**——单次预约 ≥ 机构日能力 50% 时要求 3 小时内取货，保证机构“接得下、发得出”。
3. **确认** `POST /api/v1/reservations/{id}/confirm`（店长）：生成待交接清单 `GET /reservations/{id}/handover-list`。
4. **交接**：`handover/start` → 逐箱 `handover/scan`（核对箱号/温度/封签，错扫记 `wrong_scan`、温度超标记 `temp_breach`）→ `handover/complete`（全部箱扫完 + 接收人证件核验 + 整体温度）。
5. **配送发放**：`delivery-plan`（站点箱数合计须等于交接箱数，受助对象类型须在机构能力范围内）→ `delivery-stops/{id}/complete` 逐站发放 → 全部完成自动闭环并生成**捐赠公示**。
6. **反馈**：`distributions/{id}/feedback` 受助点反馈变质 → `spoilage_report` 异常 + 对应箱标记 `spoiled`。

## 异常场景处理

| 场景 | 入口 | 系统行为 |
|---|---|---|
| 门店临时售罄 | `POST /batches/{id}/exceptions` `sold_out` | 批次取消、生效预约取消、异常凭证+事件 |
| 超过可捐赠时限 | 后台任务周期巡检（或 `POST /admin/jobs/expire`） | 批次过期、预约取消、`expired` 异常 |
| 冷链车迟到 | `truck_late` | 留痕不推进状态，门店可决定等待或按爽约处理 |
| 公益机构爽约 | `no_show` | 预约取消、批次过期 |
| 箱号错扫 | 扫码接口自动 | 记录错扫结果 + `wrong_scan` 异常，不中断交接 |
| 过敏原信息缺失 | 预约接口自动 | 阻断预约 + `allergen_missing` 异常；门店补充确认后放行 |
| 受助点反馈变质 | 反馈接口 `spoiled` | `spoilage_report` 异常 + 箱标记 spoiled |
| 监管抽查 | `regulatory_check` + `GET /batches/{id}/audit` | 抽查留痕；审计接口返回全链路轨迹并实时重算哈希链 |

## 一致性与可靠性设计

- **防重复预约三重保障**：Redis `SET NX` 批次锁 → 数据库部分唯一索引 `ux_reservations_active_batch`（同批次仅一条生效预约）→ `Idempotency-Key`（Redis 缓存 24h + 数据库唯一列兜底）。
- **状态机守卫迁移**：`UPDATE ... WHERE status=<旧状态>` 乐观并发，非法迁移直接拒绝；批次行 `SELECT ... FOR UPDATE` 串行化写操作，同时保护凭证哈希链顺序。
- **幂等**：扫码按 `(交接单, 箱号)` 唯一、交接单按预约唯一、发放按站点唯一、配送计划按预约唯一，重试均返回首次结果。
- **凭证链**：`hash = SHA256(prev_hash | type | payload | created_at)`，审计接口实时重算校验（`chain_valid`）。
- **公示一致性**：发放完成与公示投影在**同一事务**落库；事件经事务型 outbox → Kafka → consumer 二次幂等投影，兼顾强一致与流式扩展。
- **规则阻断留痕**：预约被规则拒绝时，阻断原因在独立事务落库异常（不随业务回滚），形成完整可审计轨迹。

## 目录结构

```
cmd/server/          入口 + 内嵌 openapi.yaml
internal/config/     环境变量配置
internal/domain/     实体、状态机、预约规则、凭证哈希（含单测）
internal/store/      PG 连接池、迁移执行器、事务助手
internal/service/    业务用例（批次/预约/交接/配送/异常/审计/公示/过期巡检）
internal/httpapi/    路由、中间件（认证/角色/CORS）、处理器
internal/events/     outbox relay → Kafka、consumer 公示投影
internal/jobs/       过期巡检定时任务
internal/seed/       演示数据
migrations/          SQL 迁移
scripts/verify.sh    端到端验证（79 项断言）
```
