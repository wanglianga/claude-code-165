-- 0001_init.sql 临期食品捐赠预约冷链凭证服务 - 初始 schema

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 业务单号序列
CREATE SEQUENCE batch_no_seq START 1;
CREATE SEQUENCE resv_no_seq START 1;
CREATE SEQUENCE ex_no_seq START 1;

CREATE TABLE stores (
    id            BIGSERIAL PRIMARY KEY,
    code          TEXT UNIQUE NOT NULL,
    name          TEXT NOT NULL,
    address       TEXT NOT NULL DEFAULT '',
    lat           DOUBLE PRECISION NOT NULL,
    lng           DOUBLE PRECISION NOT NULL,
    contact_phone TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE charities (
    id                       BIGSERIAL PRIMARY KEY,
    code                     TEXT UNIQUE NOT NULL,
    name                     TEXT NOT NULL,
    address                  TEXT NOT NULL DEFAULT '',
    lat                      DOUBLE PRECISION NOT NULL,
    lng                      DOUBLE PRECISION NOT NULL,
    service_radius_km        DOUBLE PRECISION NOT NULL,
    daily_capacity_boxes     INT NOT NULL CHECK (daily_capacity_boxes >= 0),
    cold_chain_vehicles      INT NOT NULL DEFAULT 0,
    cold_chain_capacity_boxes INT NOT NULL DEFAULT 0,
    recipient_types          TEXT[] NOT NULL DEFAULT '{}',
    contact_phone            TEXT NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    display_name  TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('store_staff','store_manager','charity_staff','regulator','admin')),
    store_id      BIGINT REFERENCES stores(id),
    charity_id    BIGINT REFERENCES charities(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_tokens (
    token      TEXT PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_api_tokens_user ON api_tokens(user_id);

CREATE TABLE batches (
    id                  BIGSERIAL PRIMARY KEY,
    batch_no            TEXT UNIQUE NOT NULL,
    store_id            BIGINT NOT NULL REFERENCES stores(id),
    status              TEXT NOT NULL DEFAULT 'reported' CHECK (status IN
        ('reported','reserved','confirmed','handover','handed_over','in_delivery','distributed','cancelled','expired')),
    category            TEXT NOT NULL,
    description         TEXT NOT NULL DEFAULT '',
    total_boxes         INT NOT NULL CHECK (total_boxes > 0),
    quantity_desc       TEXT NOT NULL DEFAULT '',
    produced_at         TIMESTAMPTZ NOT NULL,
    shelf_life_hours    INT NOT NULL CHECK (shelf_life_hours > 0),
    expires_at          TIMESTAMPTZ NOT NULL,
    allergens           TEXT[] NOT NULL DEFAULT '{}',
    allergen_confirmed  BOOLEAN NOT NULL DEFAULT FALSE,
    requires_cold_chain BOOLEAN NOT NULL DEFAULT FALSE,
    storage_temp_max_c  DOUBLE PRECISION,
    packaging           TEXT NOT NULL DEFAULT 'intact' CHECK (packaging IN ('intact','minor_damage','damaged')),
    address             TEXT NOT NULL DEFAULT '',
    lat                 DOUBLE PRECISION NOT NULL,
    lng                 DOUBLE PRECISION NOT NULL,
    latest_pickup_at    TIMESTAMPTZ NOT NULL,
    notes               TEXT NOT NULL DEFAULT '',
    version             INT NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_batches_status ON batches(status);
CREATE INDEX ix_batches_store ON batches(store_id);
CREATE INDEX ix_batches_pickup ON batches(latest_pickup_at) WHERE status IN ('reported','reserved','confirmed','handover');

CREATE TABLE boxes (
    id         BIGSERIAL PRIMARY KEY,
    batch_id   BIGINT NOT NULL REFERENCES batches(id),
    box_no     TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN
        ('pending','scanned','handed_over','in_delivery','distributed','spoiled','missing')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, box_no)
);
CREATE INDEX ix_boxes_batch ON boxes(batch_id);

CREATE TABLE reservations (
    id                BIGSERIAL PRIMARY KEY,
    reservation_no    TEXT UNIQUE NOT NULL,
    batch_id          BIGINT NOT NULL REFERENCES batches(id),
    charity_id        BIGINT NOT NULL REFERENCES charities(id),
    status            TEXT NOT NULL DEFAULT 'pending' CHECK (status IN
        ('pending','confirmed','cancelled','expired','completed')),
    idempotency_key   TEXT UNIQUE,
    boxes             INT NOT NULL CHECK (boxes > 0),
    planned_pickup_at TIMESTAMPTZ NOT NULL,
    vehicle_plate     TEXT NOT NULL DEFAULT '',
    driver_name       TEXT NOT NULL DEFAULT '',
    cold_chain        BOOLEAN NOT NULL DEFAULT FALSE,
    receiver_name     TEXT NOT NULL DEFAULT '',
    receiver_phone    TEXT NOT NULL DEFAULT '',
    receiver_id_no    TEXT NOT NULL DEFAULT '',
    cancel_reason     TEXT NOT NULL DEFAULT '',
    version           INT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 同一批次同一时刻只允许一条生效预约，防止重复预约（数据库层强约束）
CREATE UNIQUE INDEX ux_reservations_active_batch
    ON reservations(batch_id) WHERE status IN ('pending','confirmed');
CREATE INDEX ix_reservations_charity ON reservations(charity_id, status);
CREATE INDEX ix_reservations_batch ON reservations(batch_id);

CREATE TABLE handovers (
    id                BIGSERIAL PRIMARY KEY,
    reservation_id    BIGINT UNIQUE NOT NULL REFERENCES reservations(id),
    status            TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress','completed','aborted')),
    store_staff_id    BIGINT REFERENCES users(id),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    overall_temp_c    DOUBLE PRECISION,
    seal_intact       BOOLEAN,
    receiver_verified BOOLEAN NOT NULL DEFAULT FALSE,
    receiver_name     TEXT NOT NULL DEFAULT '',
    receiver_id_no    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE handover_scans (
    id          BIGSERIAL PRIMARY KEY,
    handover_id BIGINT NOT NULL REFERENCES handovers(id),
    box_id      BIGINT REFERENCES boxes(id),
    box_no      TEXT NOT NULL,
    result      TEXT NOT NULL CHECK (result IN ('ok','wrong_box','duplicate')),
    temp_c      DOUBLE PRECISION,
    seal_intact BOOLEAN,
    message     TEXT NOT NULL DEFAULT '',
    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 同一交接单内同一箱号只记录一次，保证扫码幂等
    UNIQUE (handover_id, box_no)
);
CREATE INDEX ix_scans_handover ON handover_scans(handover_id);

CREATE TABLE delivery_plans (
    id             BIGSERIAL PRIMARY KEY,
    reservation_id BIGINT UNIQUE NOT NULL REFERENCES reservations(id),
    vehicle_plate  TEXT NOT NULL,
    driver_name    TEXT NOT NULL,
    cold_chain     BOOLEAN NOT NULL DEFAULT FALSE,
    submitted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE delivery_stops (
    id             BIGSERIAL PRIMARY KEY,
    plan_id        BIGINT NOT NULL REFERENCES delivery_plans(id),
    seq            INT NOT NULL,
    recipient_type TEXT NOT NULL,
    site_name      TEXT NOT NULL,
    address        TEXT NOT NULL DEFAULT '',
    lat            DOUBLE PRECISION,
    lng            DOUBLE PRECISION,
    planned_boxes  INT NOT NULL CHECK (planned_boxes > 0),
    status         TEXT NOT NULL DEFAULT 'planned' CHECK (status IN ('planned','completed')),
    UNIQUE (plan_id, seq)
);

CREATE TABLE distributions (
    id              BIGSERIAL PRIMARY KEY,
    stop_id         BIGINT UNIQUE NOT NULL REFERENCES delivery_stops(id),
    reservation_id  BIGINT NOT NULL REFERENCES reservations(id),
    boxes           INT NOT NULL CHECK (boxes > 0),
    receiver_sign   TEXT NOT NULL DEFAULT '',
    distributed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    feedback_status TEXT NOT NULL DEFAULT 'none' CHECK (feedback_status IN ('none','ok','spoiled')),
    feedback_note   TEXT NOT NULL DEFAULT '',
    feedback_at     TIMESTAMPTZ
);
CREATE INDEX ix_distributions_resv ON distributions(reservation_id);

CREATE TABLE vouchers (
    id             BIGSERIAL PRIMARY KEY,
    batch_id       BIGINT NOT NULL REFERENCES batches(id),
    reservation_id BIGINT REFERENCES reservations(id),
    type           TEXT NOT NULL CHECK (type IN
        ('source','reservation','confirmation','handover','delivery','distribution','exception','closure')),
    payload        JSONB NOT NULL,
    hash           TEXT NOT NULL,
    prev_hash      TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_vouchers_batch ON vouchers(batch_id, id);

CREATE TABLE exceptions (
    id             BIGSERIAL PRIMARY KEY,
    ex_no          TEXT UNIQUE NOT NULL,
    batch_id       BIGINT REFERENCES batches(id),
    reservation_id BIGINT REFERENCES reservations(id),
    type           TEXT NOT NULL CHECK (type IN
        ('sold_out','expired','truck_late','no_show','wrong_scan','allergen_missing',
         'spoilage_report','regulatory_check','capacity_rejected','temp_breach')),
    detail         JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','resolved')),
    resolution     TEXT NOT NULL DEFAULT '',
    created_by     BIGINT REFERENCES users(id),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);
CREATE INDEX ix_exceptions_batch ON exceptions(batch_id);
CREATE INDEX ix_exceptions_status ON exceptions(status);

-- 事务型 outbox：与业务写入同库同事务，relay 负责投递到 Kafka
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;

-- 捐赠公示投影（由发放完成事件写入，供公开查询）
CREATE TABLE publicity (
    id                BIGSERIAL PRIMARY KEY,
    batch_id          BIGINT UNIQUE NOT NULL,
    batch_no          TEXT NOT NULL,
    store_name        TEXT NOT NULL,
    charity_name      TEXT NOT NULL,
    category          TEXT NOT NULL,
    boxes             INT NOT NULL,
    distributed_boxes INT NOT NULL DEFAULT 0,
    recipient_summary JSONB NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL,
    published_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
