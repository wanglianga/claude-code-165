-- 种子数据：2 家门店、3 家公益机构、账号、需求品类
-- 密码均为 Test@1234，bcrypt hash 由 migrate seed 步骤动态生成；此处只放组织与固定账号占位。
-- 实际账号密码在应用启动 seed 阶段创建（见 internal/database/seed.go）。

INSERT INTO stores (code, name, address, longitude, latitude, daily_close_time)
VALUES
  ('SH001', '麦香里·中山公园店', '上海市长宁区长宁路 100 号', 121.421000, 31.220000, '22:00'),
  ('SH002', '麦香里·徐家汇店',   '上海市徐汇区漕溪北路 1 号', 121.436500, 31.192000, '22:30')
ON CONFLICT (code) DO NOTHING;

INSERT INTO charities (code, name, license_no, address, longitude, latitude,
                       service_radius_km, daily_capacity_boxes,
                       has_cold_chain_vehicle, cold_vehicle_count)
VALUES
  ('CH001', '暖阳社区公益服务中心', 'CSH-2024-0001', '上海市长宁区愚园路 500 号', 121.425000, 31.223000,
   8.0, 40, true, 2),
  ('CH002', '晚晴养老互助社',       'CSH-2024-0002', '上海市徐汇区衡山路 20 号', 121.440000, 31.195000,
   5.0, 20, true, 1),
  ('CH003', '远郊爱心接力站',       'CSH-2024-0003', '上海市嘉定区城中路 88 号', 121.265000, 31.385000,
   3.0, 10, false, 0)
ON CONFLICT (code) DO NOTHING;

INSERT INTO charity_needs (charity_id, category)
SELECT id, c.cat FROM charities,
  (VALUES ('面包'), ('糕点'), ('乳制甜品'), ('三明治')) AS c(cat)
WHERE code = 'CH001'
ON CONFLICT DO NOTHING;

INSERT INTO charity_needs (charity_id, category)
SELECT id, c.cat FROM charities,
  (VALUES ('面包'), ('糕点')) AS c(cat)
WHERE code = 'CH002'
ON CONFLICT DO NOTHING;

-- CH003 只登记乳制甜品需求且无冷藏车，用于演示冷藏能力不足被规则拦截
INSERT INTO charity_needs (charity_id, category)
SELECT id, '乳制甜品' FROM charities WHERE code='CH003'
ON CONFLICT DO NOTHING;
