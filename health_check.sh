#!/usr/bin/env bash
# orderbook server 健康检查脚本
# 用法: ./health_check.sh [host:port]
# 默认检查 localhost:8000

set -euo pipefail

ADDR="${1:-localhost:8000}"
URL="http://${ADDR}/health"

# 超时 5 秒请求 /health
if ! RESP=$(curl -sf --max-time 5 "$URL" 2>&1); then
    echo "[FAIL] 无法连接 $URL"
    exit 1
fi

# 解析 JSON 字段（服务端响应可能带尾部空白，先清理再解析）
PARSED=$(echo "$RESP" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read().strip())
print(d.get('status',''))
print(d.get('height',''))
print(d.get('uptime_seconds',''))
print(d.get('connections',''))
" 2>/dev/null || true)

STATUS=$(echo "$PARSED" | sed -n '1p')
HEIGHT=$(echo "$PARSED" | sed -n '2p')
UPTIME=$(echo "$PARSED" | sed -n '3p')
CONNS=$(echo "$PARSED" | sed -n '4p')

if [[ -z "$STATUS" ]]; then
    echo "[FAIL] 响应格式异常: $RESP"
    exit 1
fi

if [[ "$STATUS" != "ready" ]]; then
    echo "[WARN] 服务未就绪 status=$STATUS height=$HEIGHT uptime=${UPTIME}s connections=$CONNS"
    exit 1
fi

# 检查 height 是否为 0（可能快照还没加载完）
if [[ "$HEIGHT" == "0" ]]; then
    echo "[WARN] height=0，快照可能尚未加载"
    exit 1
fi

echo "[OK] status=$STATUS height=$HEIGHT uptime=${UPTIME}s connections=$CONNS"
exit 0
