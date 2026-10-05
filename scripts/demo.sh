#!/usr/bin/env bash
# 端到端演示脚本：上报 → 规则预检 → 预约（含并发防重）→ 确认 → 到达/身份核对
# → 逐箱扫码 → 双方签收 → 温控 → 配送计划 → 逐箱发放 → 公示 → 监管抽查
# 依赖: curl jq
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
j() { jq -r "$1"; }

datefmt() {
  # GNU date 优先；macOS BSD date 回退
  date -u -d "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || {
    n="${1%% *}"; unit="${1##* }"; unit="${unit%s}"
    date -u -v"${n}${unit}" +%Y-%m-%dT%H:%M:%SZ
  }
}

login() {
  curl -s -X POST "$BASE/api/v1/auth/login" -H 'Content-Type: application/json' \
    -d "{\"login\":\"$1\",\"password\":\"Test@1234\"}" | j '.token'
}

STORE=$(login store1)
CH=$(login charity1)
REG=$(login regulator)
auth() { echo "Authorization: Bearer $1"; }

PICKUP=$(datefmt "+4 hours")     # 门店最晚取走时间
ARRIVE=$(datefmt "+2 hours")     # 机构计划 2 小时后到达（早于可捐赠时限）
SHELF=$(datefmt "+24 hours")
PROD=$(datefmt "-8 hours")

echo "== 1. 门店闭店前上报（面包4箱 + 冷藏蛋糕2箱） =="
REPORT=$(curl -s -X POST "$BASE/api/v1/batches/report" -H "$(auth "$STORE")" -H 'Content-Type: application/json' -d "{
  \"title\":\"演示批次-$(date +%H%M%S)\",\"report_type\":\"scheduled\",\"pickup_deadline\":\"$PICKUP\",
  \"items\":[
    {\"category\":\"面包\",\"name\":\"原味吐司\",\"quantity\":40,\"box_count\":4,
     \"production_at\":\"$PROD\",\"shelf_life_until\":\"$SHELF\",
     \"allergens\":[\"小麦\",\"麸质\"],\"allergen_declared\":true,
     \"needs_refrigeration\":false,\"package_integrity\":\"intact\"},
    {\"category\":\"乳制甜品\",\"name\":\"鲜奶油蛋糕\",\"quantity\":8,\"box_count\":2,
     \"production_at\":\"$PROD\",\"shelf_life_until\":\"$SHELF\",
     \"allergens\":[\"牛奶\",\"鸡蛋\",\"小麦\"],\"allergen_declared\":true,
     \"needs_refrigeration\":true,\"max_storage_temp_c\":8.0,\"package_integrity\":\"intact\"}
  ]}")
BID=$(echo "$REPORT" | j '.batch_id')
echo "batch_id=$BID  可捐赠时限=$(echo "$REPORT" | j '.donation_deadline')"

echo "== 2. 机构查看可预约批次与规则预检 =="
curl -s "$BASE/api/v1/charities/me/open-batches" -H "$(auth "$CH")" \
  | jq --arg b "$BID" '.open_batches[]|select(.batch_id==$b)|{blocked, preview:[.eligibility_preview[]|{rule,status}]}'

echo "== 3. 预约（并发 2 次，429 锁竞争自动重试，最终应一个 201、一个 409） =="
reserve_once() {
  for i in $(seq 1 30); do
    code=$(curl -s -o /tmp/rbody.$$ -w "%{http_code}" -X POST "$BASE/api/v1/reservations" \
      -H "$(auth "$CH")" -H 'Content-Type: application/json' \
      -d "{\"batch_id\":\"$BID\",\"planned_pickup_at\":\"$ARRIVE\",\"est_transit_minutes\":25}")
    [ "$code" != "429" ] && { echo "$code"; return; }
    sleep 0.1
  done
  echo 429
}
export BASE BID ARRIVE CH
rm -f /tmp/resv.1 /tmp/resv.2
( reserve_once > /tmp/resv.1 ) &
( reserve_once > /tmp/resv.2 ) &
wait
C1=$(cat /tmp/resv.1); C2=$(cat /tmp/resv.2)
echo "并发预约 HTTP 状态: $C1 / $C2（应一个 201、一个 409）"

RID=$(curl -s "$BASE/api/v1/batches/$BID" -H "$(auth "$STORE")" | j '.reservation.id')
echo "== 4. 门店确认，reservation=$RID =="
CONF=$(curl -s -X POST "$BASE/api/v1/batches/$BID/decision" -H "$(auth "$STORE")" \
  -H 'Content-Type: application/json' -d "{\"reservation_id\":\"$RID\",\"decision\":\"confirm\"}")
PCODE=$(echo "$CONF" | j '.pickup_code')
echo "pickup_code=$PCODE，封签数=$(echo "$CONF" | j '.handover_checklist|length')"

echo "== 5. 冷链车到达 + 接收人身份核对 =="
curl -s -X POST "$BASE/api/v1/reservations/$RID/arrive" -H "$(auth "$CH")" | jq '{late,late_minutes}'
curl -s -X POST "$BASE/api/v1/batches/$BID/verify-receiver" -H "$(auth "$STORE")" \
  -H 'Content-Type: application/json' \
  -d "{\"pickup_code\":\"$PCODE\",\"receiver_name\":\"王温暖\",\"receiver_id\":\"310101199003034567\",\"receiver_phone\":\"13800002222\"}" | jq

echo "== 6. 逐箱扫码核对（含错扫演示） =="
curl -s -X POST "$BASE/api/v1/batches/$BID/scan-box" -H "$(auth "$STORE")" -H 'Content-Type: application/json' \
  -d '{"box_code":"BOX-WRONG-0000","checked_qty":1}' | jq '{code,message}'
BOXES=$(curl -s "$BASE/api/v1/batches/$BID" -H "$(auth "$STORE")")
COUNT=$(echo "$BOXES" | j '.boxes|length')
for ((i=0;i<COUNT;i++)); do
  CODE=$(echo "$BOXES" | j ".boxes[$i].box_code")
  QTY=$(echo "$BOXES" | j ".boxes[$i].expected_qty")
  SEAL=$(echo "$CONF" | jq -r --arg c "$CODE" '.handover_checklist[]|select(.box_code==$c)|.seal_no')
  curl -s -X POST "$BASE/api/v1/batches/$BID/scan-box" -H "$(auth "$STORE")" -H 'Content-Type: application/json' \
    -d "{\"box_code\":\"$CODE\",\"seal_no\":\"$SEAL\",\"checked_qty\":$QTY,\"measured_temp_c\":6.2}" \
    | jq '{box_code,result}'
done

echo "== 7. 完成核对 + 双方签收 =="
curl -s -X POST "$BASE/api/v1/batches/$BID/complete-check" -H "$(auth "$STORE")" | jq
curl -s -X POST "$BASE/api/v1/batches/$BID/sign" -H "$(auth "$STORE")" -H 'Content-Type: application/json' -d '{"party":"store"}' | jq
curl -s -X POST "$BASE/api/v1/batches/$BID/sign" -H "$(auth "$CH")" -H 'Content-Type: application/json' -d '{"party":"charity"}' | jq

echo "== 8. 运输温控 + 配送计划 + 逐箱发放 =="
FIRST=$(echo "$BOXES" | j '.boxes[0].box_code')
curl -s -X POST "$BASE/api/v1/batches/$BID/temperatures" -H "$(auth "$CH")" -H 'Content-Type: application/json' \
  -d "{\"phase\":\"transport\",\"temp_c\":6.4,\"box_code\":\"$FIRST\",\"note\":\"冷链车回传\"}" | jq
HALF1=$(echo "$BOXES" | j '[.boxes[:3][].box_code]')
HALF2=$(echo "$BOXES" | j '[.boxes[3:][].box_code]')
curl -s -X POST "$BASE/api/v1/delivery-plans" -H "$(auth "$CH")" -H 'Content-Type: application/json' -d "{
  \"batch_id\":\"$BID\",\"vehicle_plate\":\"沪D·L8826\",\"driver_name\":\"李冷链\",\"cold_chain\":true,
  \"stops\":[{\"point_name\":\"华阳社区救助点\",\"point_type\":\"社区救助点\",\"box_codes\":$HALF1},
            {\"point_name\":\"晚晴养老院\",\"point_type\":\"养老院\",\"box_codes\":$HALF2}]}" | jq
for ((i=0;i<COUNT;i++)); do
  CODE=$(echo "$BOXES" | j ".boxes[$i].box_code"); QTY=$(echo "$BOXES" | j ".boxes[$i].expected_qty")
  if [ $i -lt 3 ]; then P="华阳社区救助点"; T="困难家庭"; else P="晚晴养老院"; T="住养老人"; fi
  curl -s -X POST "$BASE/api/v1/distributions" -H "$(auth "$CH")" -H 'Content-Type: application/json' \
    -d "{\"batch_id\":\"$BID\",\"point_name\":\"$P\",\"recipient_type\":\"$T\",\"box_code\":\"$CODE\",\"quantity\":$QTY}" \
    | jq '{batch_completed}'
done

echo "== 9. 捐赠公示结果（等待事件投影） =="
for i in $(seq 1 40); do
  ROW=$(curl -s "$BASE/api/v1/ledger?result=donated" -H "$(auth "$REG")" \
    | jq --arg b "$BID" '.ledger[]|select(.batch_id==$b)')
  [ -n "$ROW" ] && break
  sleep 0.5
done
echo "$ROW" | jq

echo "== 10. 监管抽查（哈希链/五证/公示一致性） =="
curl -s -X POST "$BASE/api/v1/batches/$BID/inspections" -H "$(auth "$REG")" \
  | jq '{chain_ok,five_vouchers_complete,ledger_consistent,result,findings,chain:[.vouchers[]|.voucher_type]}'
