#!/usr/bin/env bash
# 端到端验证脚本：覆盖 上报→预约→确认→交接→配送→发放→凭证→公示 主流程
# 以及 售罄/过期/迟到/爽约/错扫/过敏原缺失/变质/抽查 异常场景。
# 用法: BASE_URL=http://localhost:8080 ./scripts/verify.sh
set -u
BASE=${BASE_URL:-http://localhost:8080}
PASS=0; FAIL=0
RESP=""; CODE=""

req() { # method path token body [extra-header]
  local m=$1 p=$2 t=${3:-} b=${4:-} h=${5:-}
  local args=(-s -o /tmp/verify_resp.json -w '%{http_code}' -X "$m" "$BASE$p")
  [ -n "$t" ] && args+=(-H "Authorization: Bearer $t")
  [ -n "$h" ] && args+=(-H "$h")
  if [ -n "$b" ]; then args+=(-H 'Content-Type: application/json' -d "$b"); fi
  CODE=$(curl "${args[@]}")
  RESP=$(cat /tmp/verify_resp.json 2>/dev/null || echo '{}')
}

ok()   { PASS=$((PASS+1)); echo "  ✓ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ✗ $1"; echo "    code=$CODE resp=$(echo "$RESP" | head -c 300)"; }

expect_code() { # expected desc
  if [ "$CODE" = "$1" ]; then ok "$2 (HTTP $1)"; else bad "$2 (期望 $1)"; fi
}
assert_jq() { # desc jq-expr
  if echo "$RESP" | jq -e "$2" >/dev/null 2>&1; then ok "$1"; else bad "$1"; fi
}
login() { # username password -> echo token
  req POST /api/v1/auth/login "" "{\"username\":\"$1\",\"password\":\"$2\"}"
  echo "$RESP" | jq -r '.token // empty'
}
iso() { date -u -d "@$1" +'%Y-%m-%dT%H:%M:%SZ'; }
NOW=$(date -u +%s)

echo "== 0. 健康检查与未认证拦截 =="
req GET /healthz; expect_code 200 "healthz"
req GET /api/v1/batches; expect_code 401 "无令牌访问受保护接口返回 401"

echo "== 1. 登录各角色 =="
STORE_MGR=$(login store1.manager Store@123)
STORE_STAFF=$(login store1.staff Store@123)
CH1=$(login charity1.staff Charity@123)
CH2=$(login charity2.staff Charity@123)
REG=$(login regulator Regulator@123)
ADMIN=$(login admin Admin@123)
for v in STORE_MGR STORE_STAFF CH1 CH2 REG ADMIN; do
  if [ -n "${!v}" ]; then ok "登录 $v"; else bad "登录 $v"; fi
done
req POST /api/v1/auth/login "" '{"username":"admin","password":"wrong"}'
expect_code 401 "错误密码拒绝登录"

req GET /api/v1/stores "$STORE_MGR"; STORE1=$(echo "$RESP" | jq -r '.stores[0].id')
req GET /api/v1/charities "$CH1"
CH1_ID=$(echo "$RESP" | jq -r '.charities[0].id')
CH2_ID=$(echo "$RESP" | jq -r '.charities[1].id')
[ "$STORE1" != "null" ] && [ "$CH1_ID" != "null" ] && ok "基础数据(门店/机构)就绪" || bad "基础数据缺失"

echo "== 2. 门店上报冷链批次 A =="
BODY_A=$(jq -n \
  --arg produced "$(iso $((NOW-8*3600)))" --arg pickup "$(iso $((NOW+6*3600)))" '{
  category:"面包糕点", description:"当日吐司与可颂", total_boxes:4, quantity_desc:"每箱约20个",
  produced_at:$produced, shelf_life_hours:36, allergens:["gluten","egg","milk"],
  allergen_confirmed:true, requires_cold_chain:true, storage_temp_max_c:8,
  packaging:"intact", address:"上海市长宁区长宁路1018号", lat:31.2243, lng:121.4243,
  latest_pickup_at:$pickup, notes:"闭店前整理"}')
req POST "/api/v1/stores/$STORE1/batches" "$STORE_STAFF" "$BODY_A"
expect_code 201 "上报批次A"
BATCH_A=$(echo "$RESP" | jq -r '.batch.id')
BATCH_A_NO=$(echo "$RESP" | jq -r '.batch.batch_no')
BOX1=$(echo "$RESP" | jq -r '.boxes[0].box_no')
BOX2=$(echo "$RESP" | jq -r '.boxes[1].box_no')
BOX3=$(echo "$RESP" | jq -r '.boxes[2].box_no')
BOX4=$(echo "$RESP" | jq -r '.boxes[3].box_no')
assert_jq "批次A生成4个箱号" '.boxes | length == 4'
assert_jq "批次A状态 reported" '.batch.status == "reported"'
req POST "/api/v1/stores/$STORE1/batches" "$CH1" "$BODY_A"
expect_code 403 "机构账号不能上报批次"

echo "== 3. 预约规则校验 =="
PICKUP_PLAN=$(iso $((NOW+2*3600)))
# 无冷链车辆的机构预约冷链批次 -> 422
req POST "/api/v1/batches/$BATCH_A/reserve" "$CH2" \
  "{\"charity_id\":$CH2_ID,\"boxes\":4,\"planned_pickup_at\":\"$PICKUP_PLAN\",\"cold_chain\":false}" \
  "Idempotency-Key: rk-ch2-$NOW"
expect_code 422 "无冷藏车机构预约冷链批次被拒"
# 缺少幂等键 -> 400
req POST "/api/v1/batches/$BATCH_A/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":4,\"planned_pickup_at\":\"$PICKUP_PLAN\",\"cold_chain\":true}"
expect_code 400 "缺少 Idempotency-Key 被拒"
# 正常预约（冷链车）
req POST "/api/v1/batches/$BATCH_A/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":4,\"planned_pickup_at\":\"$PICKUP_PLAN\",\"cold_chain\":true,
    \"vehicle_plate\":\"沪A12345\",\"driver_name\":\"张三\",\"receiver_name\":\"李四\",\"receiver_id_no\":\"ID3101101990\"}" \
  "Idempotency-Key: rk-a-$NOW"
expect_code 201 "机构1预约批次A"
RESV_A=$(echo "$RESP" | jq -r '.reservation.id')
assert_jq "预约状态 pending" '.reservation.status == "pending"'
# 幂等重放
req POST "/api/v1/batches/$BATCH_A/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":4,\"planned_pickup_at\":\"$PICKUP_PLAN\",\"cold_chain\":true}" \
  "Idempotency-Key: rk-a-$NOW"
expect_code 200 "相同幂等键重放返回首次结果"
assert_jq "幂等重放标记" '.idempotent_replay == true'
# 重复预约同一批次 -> 409
req POST "/api/v1/batches/$BATCH_A/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":2,\"planned_pickup_at\":\"$PICKUP_PLAN\",\"cold_chain\":true}" \
  "Idempotency-Key: rk-a2-$NOW"
expect_code 409 "同一批次禁止重复预约"

echo "== 4. 门店确认 =="
req POST "/api/v1/reservations/$RESV_A/confirm" "$STORE_STAFF"
expect_code 403 "普通店员不能确认预约"
req POST "/api/v1/reservations/$RESV_A/confirm" "$STORE_MGR"
expect_code 200 "店长确认预约"
assert_jq "预约状态 confirmed" '.reservation.status == "confirmed"'
req GET "/api/v1/reservations/$RESV_A/handover-list" "$STORE_STAFF"
expect_code 200 "待交接清单"
assert_jq "清单含4箱待交接" '[.boxes[] | select(.status=="pending")] | length == 4'

echo "== 5. 交接扫码 =="
req POST "/api/v1/reservations/$RESV_A/handover/start" "$STORE_STAFF"
expect_code 201 "开始交接"
req POST "/api/v1/reservations/$RESV_A/handover/start" "$STORE_STAFF"
expect_code 201 "重复开始交接幂等返回"
# 箱号错扫
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  '{"box_no":"FAKE-BOX-999","temp_c":5.0,"seal_intact":true}'
assert_jq "错扫箱号记录 wrong_box" '.scan.result == "wrong_box"'
req GET "/api/v1/exceptions?batch_id=$BATCH_A" "$STORE_STAFF"
assert_jq "产生 wrong_scan 异常" '[.exceptions[] | select(.type=="wrong_scan")] | length >= 1'
# 正常扫码 + 幂等
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  "{\"box_no\":\"$BOX1\",\"temp_c\":5.0,\"seal_intact\":true}"
expect_code 201 "扫描箱1"
SCAN1_ID=$(echo "$RESP" | jq -r '.scan.id')
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  "{\"box_no\":\"$BOX1\",\"temp_c\":5.0,\"seal_intact\":true}"
expect_code 200 "重复扫码幂等返回首扫结果"
assert_jq "幂等返回同一扫码记录" ".scan.id == $SCAN1_ID"
# 温度超标
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  "{\"box_no\":\"$BOX2\",\"temp_c\":20.0,\"seal_intact\":true}"
expect_code 201 "扫描箱2(温度超标仍记录)"
req GET "/api/v1/exceptions?batch_id=$BATCH_A" "$STORE_STAFF"
assert_jq "产生 temp_breach 异常" '[.exceptions[] | select(.type=="temp_breach")] | length >= 1'
# 未扫完不能完成交接
req POST "/api/v1/reservations/$RESV_A/handover/complete" "$STORE_STAFF" \
  '{"receiver_name":"李四","receiver_id_no":"ID3101101990","overall_temp_c":6.0,"seal_intact":true}'
expect_code 422 "未扫完所有箱不能完成交接"
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  "{\"box_no\":\"$BOX3\",\"temp_c\":4.5,\"seal_intact\":true}"
req POST "/api/v1/reservations/$RESV_A/handover/scan" "$STORE_STAFF" \
  "{\"box_no\":\"$BOX4\",\"temp_c\":4.8,\"seal_intact\":true}"
# 接收人身份不符
req POST "/api/v1/reservations/$RESV_A/handover/complete" "$STORE_STAFF" \
  '{"receiver_name":"李四","receiver_id_no":"WRONG-ID","overall_temp_c":6.0,"seal_intact":true}'
expect_code 422 "接收人证件不符被拒"
req POST "/api/v1/reservations/$RESV_A/handover/complete" "$STORE_STAFF" \
  '{"receiver_name":"李四","receiver_id_no":"ID3101101990","overall_temp_c":6.0,"seal_intact":true}'
expect_code 200 "完成交接"
assert_jq "接收人身份已核验" '.handover.receiver_verified == true'

echo "== 6. 配送计划与发放 =="
# 箱数不一致 -> 422
req POST "/api/v1/reservations/$RESV_A/delivery-plan" "$CH1" \
  '{"vehicle_plate":"沪A12345","driver_name":"张三","cold_chain":true,
    "stops":[{"seq":1,"recipient_type":"community_kitchen","site_name":"华阳社区食堂","planned_boxes":3}]}'
expect_code 422 "配送箱数与交接不一致被拒"
req POST "/api/v1/reservations/$RESV_A/delivery-plan" "$CH1" \
  '{"vehicle_plate":"沪A12345","driver_name":"张三","cold_chain":true,
    "stops":[{"seq":1,"recipient_type":"community_kitchen","site_name":"华阳社区食堂","address":"华阳路20号","planned_boxes":3},
             {"seq":2,"recipient_type":"shelter","site_name":"长宁救助站","address":"凯旋路100号","planned_boxes":1}]}'
expect_code 201 "提交配送计划"
STOP1=$(echo "$RESP" | jq -r '.stops[0].id')
STOP2=$(echo "$RESP" | jq -r '.stops[1].id')
req POST "/api/v1/delivery-stops/$STOP1/complete" "$CH1" '{"boxes":3,"receiver_sign":"王五"}'
expect_code 200 "站点1发放完成"
assert_jq "批次未闭环" '.batch_distributed == false'
DIST1=$(echo "$RESP" | jq -r '.distribution.id')
req POST "/api/v1/delivery-stops/$STOP2/complete" "$CH1" '{"boxes":1,"receiver_sign":"赵六"}'
expect_code 200 "站点2发放完成"
assert_jq "批次全部发放闭环" '.batch_distributed == true'
# 幂等：重复完成站点
req POST "/api/v1/delivery-stops/$STOP2/complete" "$CH1" '{"boxes":1,"receiver_sign":"赵六"}'
expect_code 200 "重复发放幂等返回"

echo "== 7. 凭证链 / 公示 / 审计 =="
req GET "/api/v1/batches/$BATCH_A/vouchers" "$STORE_STAFF"
assert_jq "凭证链完整(chain_valid)" '.chain_valid == true'
assert_jq "含来源凭证" '[.vouchers[] | select(.type=="source")] | length == 1'
assert_jq "含交接凭证" '[.vouchers[] | select(.type=="handover")] | length == 1'
assert_jq "含发放凭证" '[.vouchers[] | select(.type=="distribution")] | length == 2'
assert_jq "含闭环凭证" '[.vouchers[] | select(.type=="closure")] | length == 1'
req GET /api/v1/publicity/donations
assert_jq "公示包含批次A" "[.donations[] | select(.batch_no==\"$BATCH_A_NO\")] | length == 1"
assert_jq "公示发放箱数=4" "[.donations[] | select(.batch_no==\"$BATCH_A_NO\")][0] | .distributed_boxes == 4"
req GET "/api/v1/batches/$BATCH_A/audit" "$STORE_STAFF"
expect_code 403 "非监管角色不能查审计"
req GET "/api/v1/batches/$BATCH_A/audit" "$REG"
expect_code 200 "监管查看全链路审计"
assert_jq "审计凭证链有效" '.chain_valid == true'

echo "== 8. 受助点反馈变质 =="
req POST "/api/v1/distributions/$DIST1/feedback" "$CH1" '{"status":"spoiled","note":"部分面包发酸"}'
expect_code 200 "提交变质反馈"
req GET "/api/v1/exceptions?batch_id=$BATCH_A" "$REG"
assert_jq "产生 spoilage_report 异常" '[.exceptions[] | select(.type=="spoilage_report")] | length == 1'
req GET "/api/v1/batches/$BATCH_A" "$REG"
assert_jq "3箱标记 spoiled" '[.boxes[] | select(.status=="spoiled")] | length == 3'
req GET "/api/v1/exceptions?batch_id=$BATCH_A&status=open" "$REG"
OPEN_EX=$(echo "$RESP" | jq -r '[.exceptions[] | select(.type=="temp_breach")][0].id')
req POST "/api/v1/exceptions/$OPEN_EX/resolve" "$REG" '{"resolution":"已要求机构加强途中温控，本批剩余食品停止发放"}'
expect_code 200 "监管关闭温度异常"

echo "== 9. 过敏原缺失阻断 + 临时售罄 =="
BODY_B=$(jq -n --arg produced "$(iso $((NOW-6*3600)))" --arg pickup "$(iso $((NOW+5*3600)))" '{
  category:"蛋糕", total_boxes:2, produced_at:$produced, shelf_life_hours:24,
  allergens:[], allergen_confirmed:false, requires_cold_chain:false, packaging:"intact",
  lat:31.2243, lng:121.4243, latest_pickup_at:$pickup}')
req POST "/api/v1/stores/$STORE1/batches" "$STORE_STAFF" "$BODY_B"
expect_code 201 "上报批次B(过敏原未确认)"
BATCH_B=$(echo "$RESP" | jq -r '.batch.id')
req POST "/api/v1/batches/$BATCH_B/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":2,\"planned_pickup_at\":\"$PICKUP_PLAN\"}" \
  "Idempotency-Key: rk-b-$NOW"
expect_code 422 "过敏原未确认禁止预约"
req GET "/api/v1/exceptions?batch_id=$BATCH_B" "$REG"
assert_jq "产生 allergen_missing 异常" '[.exceptions[] | select(.type=="allergen_missing")] | length == 1'
req PATCH "/api/v1/batches/$BATCH_B/allergens" "$STORE_STAFF" '{"allergens":["gluten","milk","nut"]}'
expect_code 200 "门店补充确认过敏原"
req POST "/api/v1/batches/$BATCH_B/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":2,\"planned_pickup_at\":\"$PICKUP_PLAN\"}" \
  "Idempotency-Key: rk-b2-$NOW"
expect_code 201 "确认过敏原后预约成功"
RESV_B=$(echo "$RESP" | jq -r '.reservation.id')
req POST "/api/v1/batches/$BATCH_B/exceptions" "$STORE_MGR" \
  '{"type":"sold_out","detail":{"reason":"门店盘点后发现已售罄"}}'
expect_code 201 "门店上报临时售罄"
req GET "/api/v1/batches/$BATCH_B" "$STORE_STAFF"
assert_jq "批次B已取消" '.batch.status == "cancelled"'
assert_jq "关联预约已取消" '.reservation.status == "cancelled"'

echo "== 10. 超过可捐赠时限自动过期 =="
NOW_C=$(date -u +%s)
BODY_C=$(jq -n --arg produced "$(iso $((NOW_C-3600)))" --arg pickup "$(iso $((NOW_C+3)))" '{
  category:"三明治", total_boxes:1, produced_at:$produced, shelf_life_hours:2,
  allergens:["egg"], allergen_confirmed:true, packaging:"intact",
  lat:31.2243, lng:121.4243, latest_pickup_at:$pickup}')
req POST "/api/v1/stores/$STORE1/batches" "$STORE_STAFF" "$BODY_C"
expect_code 201 "上报批次C(3秒后过最晚取货时间)"
BATCH_C=$(echo "$RESP" | jq -r '.batch.id')
sleep 4
req POST /api/v1/admin/jobs/expire "$ADMIN"
expect_code 200 "手动触发过期巡检"
req GET "/api/v1/batches/$BATCH_C" "$STORE_STAFF"
assert_jq "批次C已过期" '.batch.status == "expired"'
req GET "/api/v1/exceptions?batch_id=$BATCH_C" "$REG"
assert_jq "产生 expired 异常" '[.exceptions[] | select(.type=="expired")] | length == 1'

echo "== 11. 机构爽约 / 冷链车迟到 / 监管抽查 =="
BODY_D=$(jq -n --arg produced "$(iso $((NOW-4*3600)))" --arg pickup "$(iso $((NOW+6*3600)))" '{
  category:"欧包", total_boxes:2, produced_at:$produced, shelf_life_hours:30,
  allergens:["gluten"], allergen_confirmed:true, packaging:"intact",
  lat:31.2243, lng:121.4243, latest_pickup_at:$pickup}')
req POST "/api/v1/stores/$STORE1/batches" "$STORE_STAFF" "$BODY_D"
BATCH_D=$(echo "$RESP" | jq -r '.batch.id')
req POST "/api/v1/batches/$BATCH_D/reserve" "$CH1" \
  "{\"charity_id\":$CH1_ID,\"boxes\":2,\"planned_pickup_at\":\"$PICKUP_PLAN\"}" \
  "Idempotency-Key: rk-d-$NOW"
RESV_D=$(echo "$RESP" | jq -r '.reservation.id')
req POST "/api/v1/reservations/$RESV_D/confirm" "$STORE_MGR"
expect_code 200 "确认批次D预约"
req POST "/api/v1/batches/$BATCH_D/exceptions" "$CH1" \
  '{"type":"truck_late","detail":{"eta_minutes":40,"reason":"高架拥堵"}}'
expect_code 201 "机构上报冷链车迟到"
req POST "/api/v1/batches/$BATCH_D/exceptions" "$STORE_MGR" \
  '{"type":"no_show","detail":{"note":"超过约定时间未到"}}'
expect_code 201 "门店标记机构爽约"
req GET "/api/v1/batches/$BATCH_D" "$STORE_STAFF"
assert_jq "爽约后批次过期" '.batch.status == "expired"'
assert_jq "爽约后预约取消" '.reservation.status == "cancelled"'
req POST "/api/v1/batches/$BATCH_A/exceptions" "$REG" \
  '{"type":"regulatory_check","detail":{"inspector":"市监局王科","result":"凭证链完整，温度记录齐全"}}'
expect_code 201 "监管抽查留痕"
req GET "/api/v1/exceptions?batch_id=$BATCH_A" "$REG"
assert_jq "存在 regulatory_check 记录" '[.exceptions[] | select(.type=="regulatory_check")] | length == 1'

echo ""
echo "==================================="
echo "验证结果: PASS=$PASS FAIL=$FAIL"
echo "==================================="
[ "$FAIL" = "0" ]
