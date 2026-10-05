-- 爽约/迟到需要保留历史 planned_pickup_at；未来时间校验已在服务层完成。
ALTER TABLE reservations DROP CONSTRAINT IF EXISTS chk_pickup_future;
