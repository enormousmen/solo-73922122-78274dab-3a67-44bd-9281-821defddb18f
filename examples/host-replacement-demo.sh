#!/usr/bin/env bash
# 节点换机端到端演示（配合 docker compose 启动的服务使用）。
#
# 演示内容：
#   1. 全量健康上报 -> 申请重启许可（占用 n1）
#   2. 凭未结许可为 n1 登记换机，领取探活来源编号；重复登记只返回原编号
#   3. 旧主机报告（漏填/错填编号）整批拒收，且不影响同批其他节点
#   4. 登记后首份新报告到达前，n1 视为健康未知
#   5. 新主机带当前编号上报，凭登记后采集的新探活结单
#
# 用法：
#   docker compose up --build -d
#   ./examples/host-replacement-demo.sh            # 默认 http://localhost:8000
#   BASE_URL=http://127.0.0.1:8000 ./examples/host-replacement-demo.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
# 每次演示用唯一申请号，可对同一服务重复执行
RID="demo-req-$(date +%s)"

# JSON 取值：$1=JSON 文本，$2=Python 下标表达式
# （如 ['replacements'][0]['source_id']）；镜像与常见环境均自带 python3
json_get() { python3 -c "import json,sys; print(eval(\"json.load(sys.stdin)$2\"))" <<<"$1"; }

# 发请求并打印：$1=期望状态码，其后为 curl 参数
request() {
  local expected="$1"; shift
  local body
  local code
  code=$(curl -sS -o /tmp/demo-body.$$ -w '%{http_code}' "$@")
  body=$(cat /tmp/demo-body.$$)
  rm -f /tmp/demo-body.$$
  printf '%s\n' "$body"
  if [[ "$code" != "$expected" ]]; then
    echo "!! 期望 HTTP $expected，实际 $code" >&2
    exit 1
  fi
}

hr() { printf '\n========== %s ==========\n' "$1"; }

hr "0. 健康检查"
request 200 "$BASE_URL/healthz"

hr "1. 五个节点上报健康探活（未换机，不带来源编号）"
request 201 -X POST "$BASE_URL/liveness" -H 'Content-Type: application/json' -d '{
  "reports": [
    {"node_id":"n1","sampled_at":"2026-11-20T09:00:00Z","healthy":true,"boot_id":"boot-1"},
    {"node_id":"n2","sampled_at":"2026-11-20T09:00:00Z","healthy":true,"boot_id":"boot-1"},
    {"node_id":"n3","sampled_at":"2026-11-20T09:00:00Z","healthy":true,"boot_id":"boot-1"},
    {"node_id":"n4","sampled_at":"2026-11-20T09:00:00Z","healthy":true,"boot_id":"boot-1"},
    {"node_id":"n5","sampled_at":"2026-11-20T09:00:00Z","healthy":true,"boot_id":"boot-1"}
  ]}'

hr "2. 申请重启许可（本批 n1）"
request 201 -X POST "$BASE_URL/permits" -H 'Content-Type: application/json' \
  -d "{\"request_id\":\"$RID\",\"nodes\":[\"n1\"]}"

hr "3. 凭未结许可为 n1 登记换机 -> 201，发放新的探活来源编号"
body=$(request 201 -X POST "$BASE_URL/permits/$RID/replacements" \
  -H 'Content-Type: application/json' -d '{"nodes":["n1"]}')
echo "$body"
SRC=$(json_get "$body" "['replacements'][0]['source_id']")
echo ">> 领取到来源编号: $SRC"

hr "4. 同一许可重复登记同一节点 -> 200，只返回原编号"
body=$(request 200 -X POST "$BASE_URL/permits/$RID/replacements" \
  -H 'Content-Type: application/json' -d '{"nodes":["n1"]}')
echo "$body"
SRC_AGAIN=$(json_get "$body" "['replacements'][0]['source_id']")
[[ "$SRC" == "$SRC_AGAIN" ]] || { echo "!! 幂等失败：编号变化" >&2; exit 1; }
echo ">> 编号保持不变: $SRC_AGAIN"

hr "5. 旧主机报告拒收：漏填来源编号 -> 422，整批不入库"
request 422 -X POST "$BASE_URL/liveness" -H 'Content-Type: application/json' -d '{
  "reports":[{"node_id":"n1","sampled_at":"2026-11-20T10:00:00Z",
              "healthy":true,"boot_id":"boot-old"}]}'

hr "6. 旧主机报告拒收：填错来源编号 -> 422"
request 422 -X POST "$BASE_URL/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[{\"node_id\":\"n1\",\"sampled_at\":\"2026-11-20T10:01:00Z\",
               \"healthy\":true,\"boot_id\":\"boot-old\",\"source_id\":\"src-wrong\"}]}"

hr "7. 混合批次整批拒收：n1 来源不符，连 n2 的合法报告也不入库"
n2_before=$(request 200 "$BASE_URL/liveness/n2")
echo "n2 拒收前: $n2_before"
request 422 -X POST "$BASE_URL/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[
    {\"node_id\":\"n1\",\"sampled_at\":\"2026-11-20T10:02:00Z\",\"healthy\":true,\"boot_id\":\"boot-old\"},
    {\"node_id\":\"n2\",\"sampled_at\":\"2026-11-20T10:02:00Z\",\"healthy\":true,\"boot_id\":\"boot-new\"}
  ]}"
n2_after=$(request 200 "$BASE_URL/liveness/n2")
echo "n2 拒收后: $n2_after"
[[ "$n2_before" == "$n2_after" ]] || { echo "!! 整批语义失效：n2 被写入" >&2; exit 1; }
echo ">> n2 报告未被覆盖，整批回滚生效"

hr "8. 首份新探活到达前：n1 健康未知"
request 404 "$BASE_URL/liveness/n1"
listing=$(request 200 "$BASE_URL/liveness")
echo "$listing"
python3 -c "import json,sys; assert 'n1' in json.load(sys.stdin)['nodes_without_reports']" <<<"$listing" \
  || { echo "!! n1 应出现在 nodes_without_reports" >&2; exit 1; }
echo ">> n1 当前无带当前编号的报告，按健康未知处理"

hr "9. 新主机带当前编号上报（采样时间晚于登记时间）-> 201"
# 动态取一个晚于当前（必晚于登记时刻）的 UTC 时间
FRESH_TS=$(python3 -c "from datetime import datetime,timedelta,timezone; print((datetime.now(timezone.utc)+timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
request 201 -X POST "$BASE_URL/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[{\"node_id\":\"n1\",\"sampled_at\":\"$FRESH_TS\",
               \"healthy\":true,\"boot_id\":\"boot-2\",\"source_id\":\"$SRC\"}]}"
request 200 "$BASE_URL/liveness/n1"

hr "10. 结单：新探活在登记后采集、健康、启动标识已变 -> 200"
request 200 -X POST "$BASE_URL/permits/$RID/finish"

hr "演示完成：旧主机报告全程拒收，凭新探活来源编号完成结单"
