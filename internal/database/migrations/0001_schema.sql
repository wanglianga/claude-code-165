-- 连锁烘焙门店临期食品捐赠预约冷链凭证服务 - 领域模型
-- PostgreSQL 13+

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============ 身份与组织 ============
CREATE TABLE users (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    login           text NOT NULL UNIQUE,
    password_hash   text NOT NULL,
    display_name    text NOT NULL,
    role            text NOT NULL CHECK (role IN ('platform_admin','store_staff','charity_staff','regulator')),
    store_id        uuid,
    charity_id      uuid,
    status          text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stores (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code            text NOT NULL UNIQUE,
    name            text NOT NULL,
    address         text NOT NULL,
    longitude       numeric(10,6) NOT NULL,
    latitude        numeric(10,6) NOT NULL,
    daily_close_time time NOT NULL DEFAULT '22:00',
    status          text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE charities (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code                    text NOT NULL UNIQUE,
    name                    text NOT NULL,
    license_no              text NOT NULL UNIQUE,          -- 公益接收资质编号
    address                 text NOT NULL,
    longitude               numeric(10,6) NOT NULL,
    latitude                numeric(10,6) NOT NULL,
    service_radius_km       numeric(6,2) NOT NULL,         -- 服务半径
    daily_capacity_boxes    integer NOT NULL CHECK (daily_capacity_boxes > 0), -- 日接收能力(箱)
    has_cold_chain_vehicle  boolean NOT NULL DEFAULT false,-- 是否有冷藏车
    cold_vehicle_count      integer NOT NULL DEFAULT 0,
    reliability_score       numeric(4,3) NOT NULL DEFAULT 0.900 CHECK (reliability_score BETWEEN 0 AND 1),
    completed_count         integer NOT NULL DEFAULT 0,
    no_show_count           integer NOT NULL DEFAULT 0,
    late_count              integer NOT NULL DEFAULT 0,
    status                  text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
    created_at              timestamptz NOT NULL DEFAULT now()
);

-- 公益机构的受助人需求品类
CREATE TABLE charity_needs (
    charity_id  uuid NOT NULL REFERENCES charities(id) ON DELETE CASCADE,
    category    text NOT NULL,
    PRIMARY KEY (charity_id, category)
);

ALTER TABLE users ADD CONSTRAINT fk_users_store FOREIGN KEY (store_id) REFERENCES stores(id);
ALTER TABLE users ADD CONSTRAINT fk_users_charity FOREIGN KEY (charity_id) REFERENCES charities(id);

-- ============ 门店上报：批次 / 品类行 / 箱体 ============
-- 批次状态机:
-- open(可预约) -> reserved(已预约待门店确认) -> confirmed(已确认,待交接)
--   -> in_handover(交接中) -> in_transit(运输中) -> delivered(已送达) -> distributed(已发放)
-- 异常终态/分支: released(重新开放预约), sold_out(临时售罄), expired(超过可捐赠时限),
--   cancelled(机构撤销且无再预约), quarantined(变质封存)
CREATE TABLE donation_batches (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code                    text NOT NULL UNIQUE,
    store_id                uuid NOT NULL REFERENCES stores(id),
    report_type             text NOT NULL DEFAULT 'scheduled' CHECK (report_type IN ('scheduled','emergency')),
    title                   text NOT NULL,
    -- 门店自报的最晚取走时间
    pickup_deadline         timestamptz NOT NULL,
    -- min(各品类可捐赠时限=保质期-安全缓冲, 最晚取走时间)，由服务计算并强制
    donation_deadline       timestamptz NOT NULL,
    total_boxes             integer NOT NULL DEFAULT 0,
    total_quantity          integer NOT NULL DEFAULT 0,
    needs_refrigeration     boolean NOT NULL DEFAULT false,
    allergen_complete       boolean NOT NULL DEFAULT true,
    -- open/reserved/confirmed/in_handover/in_transit/delivered/distributed/released/sold_out/expired/cancelled/quarantined
    status                  text NOT NULL DEFAULT 'open',
    reported_by             uuid NOT NULL REFERENCES users(id),
    confirmed_at            timestamptz,
    handed_over_at          timestamptz,
    delivered_at            timestamptz,
    distributed_at          timestamptz,
    closed_at               timestamptz,
    close_reason            text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_batch_status CHECK (status IN
        ('open','reserved','confirmed','in_handover','in_transit','delivered','distributed',
         'released','sold_out','expired','cancelled','quarantined'))
);
CREATE INDEX idx_batches_store ON donation_batches(store_id, created_at DESC);
CREATE INDEX idx_batches_status ON donation_batches(status);
CREATE INDEX idx_batches_deadline ON donation_batches(donation_deadline);

CREATE TABLE batch_items (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                uuid NOT NULL REFERENCES donation_batches(id) ON DELETE CASCADE,
    category                text NOT NULL,              -- 品类: 面包/糕点/乳制甜品...
    name                    text NOT NULL,
    quantity                integer NOT NULL CHECK (quantity > 0),     -- 总份数
    box_count               integer NOT NULL CHECK (box_count > 0),    -- 箱数
    production_at           timestamptz NOT NULL,       -- 生产时间
    shelf_life_until        timestamptz NOT NULL,       -- 保质期
    allergens               text[] NOT NULL DEFAULT '{}',
    allergen_declared       boolean NOT NULL DEFAULT true, -- 过敏原信息是否已明确(缺失=false)
    needs_refrigeration     boolean NOT NULL DEFAULT false,
    max_storage_temp_c      numeric(4,1),               -- 冷藏上限温度(如 8.0℃)
    package_integrity       text NOT NULL CHECK (package_integrity IN ('intact','repacked','damaged')),
    quantity_reserved       integer NOT NULL DEFAULT 0,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_item_qty CHECK (quantity_reserved <= quantity)
);
CREATE INDEX idx_items_batch ON batch_items(batch_id);

CREATE TABLE boxes (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    box_code        text NOT NULL UNIQUE,
    batch_id        uuid NOT NULL REFERENCES donation_batches(id) ON DELETE CASCADE,
    item_id         uuid NOT NULL REFERENCES batch_items(id) ON DELETE CASCADE,
    seq_in_item     integer NOT NULL,
    expected_qty    integer NOT NULL,                 -- 应交份数
    seal_no         text,                             -- 封签号(确认预约时由门店启用)
    -- pending/scanned_ok/rejected/delivered/distributed/quarantined
    scan_status     text NOT NULL DEFAULT 'pending',
    scanned_at      timestamptz,
    measured_temp_c numeric(4,1),
    seal_ok         boolean,
    checked_qty     integer,
    reject_reason   text,
    delivered_at    timestamptz,
    distributed_at  timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_box_status CHECK (scan_status IN
        ('pending','scanned_ok','rejected','delivered','distributed','quarantined'))
);
CREATE INDEX idx_boxes_batch ON boxes(batch_id);

-- ============ 预约 ============
-- requested(待确认) / confirmed(已确认) / rejected / cancelled / no_show / auto_expired
CREATE TABLE reservations (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code                text NOT NULL UNIQUE,
    batch_id            uuid NOT NULL REFERENCES donation_batches(id),
    charity_id          uuid NOT NULL REFERENCES charities(id),
    planned_pickup_at   timestamptz NOT NULL,        -- 计划到达取货时间
    est_transit_minutes integer NOT NULL,            -- 预估运输时长(用于发放时限判断)
    demand_categories   text[] NOT NULL DEFAULT '{}',-- 匹配的受助人需求
    -- 预约时的能力快照（半径/运力/冷藏/当日剩余运力），便于审计
    capability_snapshot jsonb NOT NULL,
    eligibility_trace   jsonb NOT NULL DEFAULT '[]', -- 规则命中明细
    pickup_code         text,                        -- 取货身份码(确认时生成)
    status              text NOT NULL DEFAULT 'requested',
    requested_by        uuid NOT NULL REFERENCES users(id),
    requested_at        timestamptz NOT NULL DEFAULT now(),
    decided_at          timestamptz,
    decided_by          uuid REFERENCES users(id),
    reject_reason       text,
    arrived_at          timestamptz,                 -- 冷链车实际到达
    late_minutes        integer,
    CONSTRAINT chk_resv_status CHECK (status IN
        ('requested','confirmed','rejected','cancelled','no_show','auto_expired')),
    CONSTRAINT chk_pickup_future CHECK (planned_pickup_at > now())
);
-- 同一批次同时只能存在一个有效预约 —— 防重复预约的数据库硬约束
CREATE UNIQUE INDEX uq_active_reservation ON reservations(batch_id)
    WHERE status IN ('requested','confirmed');
CREATE INDEX idx_resv_charity ON reservations(charity_id, requested_at DESC);

-- ============ 交接 ============
-- pending(待交接) / checked(核对完成) / signed(双方签收) / failed
CREATE TABLE handovers (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            uuid NOT NULL UNIQUE REFERENCES donation_batches(id),
    reservation_id      uuid NOT NULL UNIQUE REFERENCES reservations(id),
    receiver_name       text,                        -- 接收人
    receiver_id_masked  text,                        -- 证件脱敏留存
    receiver_phone_mask text,
    store_confirmed_by  uuid REFERENCES users(id),
    store_signed_at     timestamptz,
    charity_signed_at   timestamptz,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','checked','signed','failed')),
    completed_at        timestamptz,
    note                text,
    created_at          timestamptz NOT NULL DEFAULT now()
);

-- 温控记录（交接/运输/送达三个环节，逐箱或整批）
CREATE TABLE temperature_readings (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id    uuid NOT NULL REFERENCES donation_batches(id),
    box_id      uuid REFERENCES boxes(id),
    phase       text NOT NULL CHECK (phase IN ('handover','transport','recipient')),
    temp_c      numeric(4,1) NOT NULL,
    limit_temp_c numeric(4,1),
    compliant   boolean NOT NULL,
    recorded_by uuid REFERENCES users(id),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    note        text
);
CREATE INDEX idx_temp_batch ON temperature_readings(batch_id, recorded_at);

-- ============ 配送计划 ============
CREATE TABLE delivery_plans (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        uuid NOT NULL UNIQUE REFERENCES donation_batches(id),
    charity_id      uuid NOT NULL REFERENCES charities(id),
    vehicle_plate   text NOT NULL,
    driver_name     text NOT NULL,
    cold_chain      boolean NOT NULL,
    uploaded_by     uuid NOT NULL REFERENCES users(id),
    uploaded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE delivery_stops (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id         uuid NOT NULL REFERENCES delivery_plans(id) ON DELETE CASCADE,
    seq             integer NOT NULL,
    point_name      text NOT NULL,                   -- 受助点名称
    point_type      text NOT NULL,                   -- 养老院/社区救助点/困难家庭...
    longitude       numeric(10,6),
    latitude        numeric(10,6),
    eta             timestamptz,
    box_codes       text[] NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (plan_id, seq)
);

-- ============ 最终发放 ============
CREATE TABLE distributions (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            uuid NOT NULL REFERENCES donation_batches(id),
    stop_id             uuid REFERENCES delivery_stops(id),
    recipient_type      text NOT NULL,               -- 最终发放对象类型
    point_name          text NOT NULL,
    box_code            text NOT NULL,
    quantity            integer NOT NULL,
    distributed_at      timestamptz NOT NULL DEFAULT now(),
    recorded_by         uuid NOT NULL REFERENCES users(id),
    -- ok / spoiled(受助点反馈变质) / other
    feedback_status     text NOT NULL DEFAULT 'ok' CHECK (feedback_status IN ('ok','spoiled','other')),
    feedback_note       text,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_dist_batch ON distributions(batch_id);

-- ============ 凭证（哈希链，每批次一条链） ============
-- source(来源) / temperature(温控) / handover(交接) / delivery(配送) / distribution(发放)
CREATE TABLE vouchers (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        uuid NOT NULL REFERENCES donation_batches(id),
    seq             integer NOT NULL,
    voucher_type    text NOT NULL CHECK (voucher_type IN
                        ('source','temperature','handover','delivery','distribution','exception','inspection')),
    title           text NOT NULL,
    payload         jsonb NOT NULL,                  -- 凭证内容快照
    payload_hash    text NOT NULL,                   -- sha256(规范化payload)
    prev_hash       text NOT NULL,                   -- 上一链节(创世=GENESIS)
    chain_hash      text NOT NULL,                   -- sha256(prev_hash|payload_hash)
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (batch_id, seq)
);
CREATE INDEX idx_voucher_batch ON vouchers(batch_id, seq);

-- ============ 异常登记 ============
-- sold_out / past_deadline / cold_truck_late / no_show / wrong_scan /
-- allergen_missing / temp_breach / recipient_spoiled
CREATE TABLE exception_records (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id    uuid REFERENCES donation_batches(id),
    ex_type     text NOT NULL CHECK (ex_type IN
                    ('sold_out','past_deadline','cold_truck_late','no_show',
                     'wrong_scan','allergen_missing','temp_breach','recipient_spoiled')),
    detail      jsonb NOT NULL DEFAULT '{}',
    blocking    boolean NOT NULL DEFAULT false,     -- 是否阻断交接
    resolved    boolean NOT NULL DEFAULT false,
    resolution  text,
    raised_by   uuid REFERENCES users(id),
    raised_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);
CREATE INDEX idx_ex_batch ON exception_records(batch_id);
CREATE INDEX idx_ex_open ON exception_records(ex_type, resolved) WHERE resolved = false;

-- ============ 监管抽查 ============
CREATE TABLE inspections (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        uuid NOT NULL REFERENCES donation_batches(id),
    inspector_id    uuid NOT NULL REFERENCES users(id),
    inspected_at    timestamptz NOT NULL DEFAULT now(),
    chain_ok        boolean NOT NULL,
    five_vouchers_complete boolean NOT NULL,
    ledger_consistent boolean NOT NULL,
    result          text NOT NULL CHECK (result IN ('pass','defect')),
    findings        jsonb NOT NULL DEFAULT '[]',
    note            text
);
CREATE INDEX idx_insp_batch ON inspections(batch_id, inspected_at DESC);

-- ============ Outbox / 消费幂等 / 公示读模型 ============
CREATE TABLE outbox_events (
    id              bigserial PRIMARY KEY,
    event_id        uuid NOT NULL UNIQUE,
    aggregate_type  text NOT NULL,
    aggregate_id    uuid NOT NULL,
    event_type      text NOT NULL,
    payload         jsonb NOT NULL,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id        uuid PRIMARY KEY,
    processed_at    timestamptz NOT NULL DEFAULT now()
);

-- 捐赠公示（由事件投影维护，可随时从权威表重建对账）
CREATE TABLE public_ledger (
    batch_id            uuid PRIMARY KEY REFERENCES donation_batches(id),
    batch_code          text NOT NULL,
    store_name          text NOT NULL,
    charity_name        text,
    title               text NOT NULL,
    categories          text NOT NULL,
    total_quantity      integer NOT NULL,
    total_boxes         integer NOT NULL,
    report_type         text NOT NULL,
    status              text NOT NULL,
    result              text NOT NULL DEFAULT 'in_progress', -- in_progress/donated/wasted/spoiled/cancelled
    reported_at         timestamptz NOT NULL,
    handed_over_at      timestamptz,
    distributed_at      timestamptz,
    vouchers_complete   boolean NOT NULL DEFAULT false,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- ============ 通用触发器 ============
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_batches_updated BEFORE UPDATE ON donation_batches
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
