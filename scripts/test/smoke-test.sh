#!/usr/bin/env bash
# =============================================================================
# smoke-test.sh - 冒煙測試：驗證叢集的核心功能是否正常
#
# 測項：
#   1. 叢集可連線、broker 清單正確
#   2. metadata quorum 健康
#   3. 建立 topic
#   4. 生產訊息
#   5. 消費訊息並比對內容
#   6. Consumer group offset 正確提交
#   7. Key 相同的訊息落在同一 partition（順序保證）
#   8. 副本與 ISR 狀態
#   9. 設定變更（動態 topic config）
#  10. 清理
#
# 用法：
#   ./scripts/test/smoke-test.sh
#   KEEP_TOPIC=true ./scripts/test/smoke-test.sh   # 保留測試 topic 以便觀察
#
# 退出碼：0 = 全部通過，1 = 有測項失敗
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi
[[ $# -gt 0 ]] && die "smoke-test.sh 不接受位置參數（用環境變數設定，見 --help）：$*"

TOPIC="${TEST_TOPIC:-smoke-test-$(date +%s)}"
GROUP="${TEST_GROUP:-smoke-group-$(date +%s)}"
MSG_COUNT="${MSG_COUNT:-100}"
PARTITIONS="${TEST_PARTITIONS:-3}"
KEEP_TOPIC="${KEEP_TOPIC:-false}"
WORK_DIR="$(mktemp -d)"

PASS=0; FAIL=0
declare -a FAILED_TESTS=()

ok()   { PASS=$(( PASS + 1 )); printf '  %s✔ PASS%s %s\n' "${C_GRN}" "${C_RESET}" "$*"; }
ng()   { FAIL=$(( FAIL + 1 )); FAILED_TESTS+=("$*"); printf '  %s✘ FAIL%s %s\n' "${C_RED}" "${C_RESET}" "$*"; }
info() { printf '  %s· %s%s\n' "${C_DIM}" "$*" "${C_RESET}"; }

cleanup() {
  local rc=$?
  if [[ "${KEEP_TOPIC}" != "true" ]]; then
    kafka_topics --delete --topic "${TOPIC}" >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK_DIR}"
  exit "${rc}"
}
trap cleanup EXIT

# 副本數不能超過 broker 數
detect_replication_factor() {
  local brokers
  brokers="$("$(kafka_bin kafka-broker-api-versions.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} 2>/dev/null \
      | grep -cE '^\S+:[0-9]+ \(id:' || true)"
  (( brokers < 1 )) && brokers=1
  (( brokers > 3 )) && brokers=3
  printf '%s' "${brokers}"
}

section "冒煙測試"
info "bootstrap : ${BOOTSTRAP_SERVERS}"
info "topic     : ${TOPIC}"
info "group     : ${GROUP}"
info "訊息數    : ${MSG_COUNT}"

# -----------------------------------------------------------------------------
section "測試 1：叢集連線"
if cluster_ready; then
  BROKER_LIST="$("$(kafka_bin kafka-broker-api-versions.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} 2>/dev/null \
      | grep -E '^\S+:[0-9]+ \(id:' || true)"
  BROKER_COUNT="$(printf '%s\n' "${BROKER_LIST}" | grep -c 'id:' || true)"
  ok "叢集可連線，偵測到 ${BROKER_COUNT} 個 broker"
  printf '%s\n' "${BROKER_LIST}" | sed 's/^/      /'
else
  ng "無法連線到 ${BOOTSTRAP_SERVERS}"
  log_error "叢集未就緒，後續測試無意義，中止"
  exit 1
fi

RF="$(detect_replication_factor)"
info "本次測試使用 replication.factor=${RF}"

# -----------------------------------------------------------------------------
section "測試 2：Metadata quorum（KRaft）"
if QUORUM="$(kafka_metadata describe --status 2>/dev/null)"; then
  LEADER_ID="$(printf '%s' "${QUORUM}" | awk -F: '/LeaderId/{gsub(/ /,"",$2); print $2}')"
  if [[ -n "${LEADER_ID}" && "${LEADER_ID}" != "-1" ]]; then
    ok "quorum 有 leader（LeaderId=${LEADER_ID}）"
  else
    ng "quorum 沒有 leader，叢集無法接受 metadata 變更"
  fi
  printf '%s\n' "${QUORUM}" | sed 's/^/      /'
else
  ng "無法查詢 metadata quorum（非 KRaft 叢集或權限不足？）"
fi

# -----------------------------------------------------------------------------
section "測試 3：建立 topic"
if kafka_topics --create --topic "${TOPIC}" \
      --partitions "${PARTITIONS}" --replication-factor "${RF}" \
      --config retention.ms=3600000 >/dev/null 2>&1; then
  ok "建立 topic ${TOPIC}（${PARTITIONS} partitions, RF=${RF}）"
else
  ng "建立 topic 失敗"
  exit 1
fi

if kafka_topics --list 2>/dev/null | grep -qx "${TOPIC}"; then
  ok "topic 出現在清單中"
else
  ng "topic 未出現在 --list 結果中"
fi

# -----------------------------------------------------------------------------
section "測試 4：生產訊息"
PRODUCE_FILE="${WORK_DIR}/produce.txt"
for i in $(seq 1 "${MSG_COUNT}"); do
  printf 'key-%d:{"seq":%d,"payload":"smoke-test-message"}\n' "$(( i % 5 ))" "${i}"
done > "${PRODUCE_FILE}"

if "$(kafka_bin kafka-console-producer.sh)" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      ${KAFKA_CLIENT_CONFIG:+--producer.config "${KAFKA_CLIENT_CONFIG}"} \
      --topic "${TOPIC}" \
      --property "parse.key=true" --property "key.separator=:" \
      --request-required-acks all \
      < "${PRODUCE_FILE}" >/dev/null 2>&1; then
  ok "以 acks=all 送出 ${MSG_COUNT} 筆訊息"
else
  ng "生產訊息失敗"
fi

# 用 offset 確認訊息真的落地
OFFSETS="$("$(kafka_bin kafka-get-offsets.sh)" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} \
    --topic "${TOPIC}" 2>/dev/null || true)"
TOTAL_OFFSET="$(printf '%s\n' "${OFFSETS}" | awk -F: '{s+=$3} END{print s+0}')"
if (( TOTAL_OFFSET == MSG_COUNT )); then
  ok "各 partition end offset 總和 = ${TOTAL_OFFSET}，與送出筆數相符"
else
  ng "end offset 總和 = ${TOTAL_OFFSET}，預期 ${MSG_COUNT}"
fi
printf '%s\n' "${OFFSETS}" | sed 's/^/      /'

# -----------------------------------------------------------------------------
section "測試 5：消費訊息"
CONSUME_FILE="${WORK_DIR}/consume.txt"
"$(kafka_bin kafka-console-consumer.sh)" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    ${KAFKA_CLIENT_CONFIG:+--consumer.config "${KAFKA_CLIENT_CONFIG}"} \
    --topic "${TOPIC}" \
    --group "${GROUP}" \
    --from-beginning \
    --max-messages "${MSG_COUNT}" \
    --timeout-ms 30000 \
    > "${CONSUME_FILE}" 2>/dev/null || true

CONSUMED="$(grep -c 'smoke-test-message' "${CONSUME_FILE}" || true)"
if (( CONSUMED == MSG_COUNT )); then
  ok "消費到 ${CONSUMED} 筆，與生產筆數相符（無遺失、無重複）"
else
  ng "消費到 ${CONSUMED} 筆，預期 ${MSG_COUNT}"
fi

# 內容完整性：所有 seq 1..N 都要出現且不重複
MISSING="$(comm -23 \
  <(seq 1 "${MSG_COUNT}" | sort) \
  <(grep -oE '"seq":[0-9]+' "${CONSUME_FILE}" | cut -d: -f2 | sort -u) | tr '\n' ' ')"
if [[ -z "${MISSING// /}" ]]; then
  ok "seq 1..${MSG_COUNT} 全數到齊，內容完整"
else
  ng "缺少 seq：${MISSING}"
fi

# -----------------------------------------------------------------------------
section "測試 6：Consumer group offset"
GROUP_DESC="$(kafka_groups --describe --group "${GROUP}" 2>/dev/null || true)"
if printf '%s' "${GROUP_DESC}" | grep -q "${TOPIC}"; then
  TOTAL_LAG="$(printf '%s\n' "${GROUP_DESC}" | awk -v t="${TOPIC}" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')"
  if (( TOTAL_LAG == 0 )); then
    ok "consumer group offset 已提交，總 lag = 0"
  else
    ng "總 lag = ${TOTAL_LAG}，預期 0（offset 未正確提交？）"
  fi
  printf '%s\n' "${GROUP_DESC}" | sed 's/^/      /'
else
  ng "找不到 consumer group ${GROUP} 的 offset 記錄"
fi

# -----------------------------------------------------------------------------
section "測試 7：Key 分區與順序保證"
# 相同 key 必須永遠落在同一個 partition，這是 Kafka 順序保證的基礎
KEY_PARTITION_FILE="${WORK_DIR}/keypart.txt"
"$(kafka_bin kafka-console-consumer.sh)" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    ${KAFKA_CLIENT_CONFIG:+--consumer.config "${KAFKA_CLIENT_CONFIG}"} \
    --topic "${TOPIC}" --from-beginning --max-messages "${MSG_COUNT}" \
    --timeout-ms 30000 \
    --property print.key=true --property print.partition=true \
    > "${KEY_PARTITION_FILE}" 2>/dev/null || true

# 輸出格式為： Partition:<n><TAB><key><TAB><value>
VIOLATION_REPORT="$(awk -F'\t' '
  $1 ~ /^Partition:/ && NF >= 3 {
    part = substr($1, index($1, ":") + 1)
    key  = $2
    if (!( (key SUBSEP part) in seen )) { seen[key SUBSEP part] = 1; n[key]++ }
  }
  END { for (k in n) if (n[k] > 1) printf "%s 出現在 %d 個 partition\n", k, n[k] }
' "${KEY_PARTITION_FILE}")"
VIOLATIONS="$(printf '%s' "${VIOLATION_REPORT}" | grep -c . || true)"
[[ -n "${VIOLATION_REPORT}" ]] && printf '%s\n' "${VIOLATION_REPORT}" | sed 's/^/      /' || true

if (( VIOLATIONS == 0 )); then
  ok "每個 key 都固定落在單一 partition（順序保證成立）"
else
  ng "${VIOLATIONS} 個 key 分散在多個 partition"
fi

# -----------------------------------------------------------------------------
section "測試 8：副本與 ISR"
DESC="$(kafka_topics --describe --topic "${TOPIC}" 2>/dev/null || true)"
printf '%s\n' "${DESC}" | sed 's/^/      /'
UNDER_REPLICATED="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c "${TOPIC}" || true)"
if (( UNDER_REPLICATED == 0 )); then
  ok "沒有 under-replicated partition"
else
  ng "有 ${UNDER_REPLICATED} 個 under-replicated partition"
fi
NO_LEADER="$(printf '%s\n' "${DESC}" | grep -c 'Leader: none' || true)"
if (( NO_LEADER == 0 )); then
  ok "所有 partition 都有 leader"
else
  ng "有 ${NO_LEADER} 個 partition 沒有 leader"
fi

# -----------------------------------------------------------------------------
section "測試 9：動態設定變更"
if kafka_configs --entity-type topics --entity-name "${TOPIC}" \
      --alter --add-config retention.ms=7200000 >/dev/null 2>&1; then
  CURRENT="$(kafka_configs --entity-type topics --entity-name "${TOPIC}" --describe 2>/dev/null | grep -o 'retention.ms=[0-9]*' | head -1)"
  if [[ "${CURRENT}" == "retention.ms=7200000" ]]; then
    ok "動態變更 topic 設定生效（${CURRENT}）"
  else
    ng "設定變更未生效，實際為 ${CURRENT:-<空>}"
  fi
else
  ng "kafka-configs --alter 執行失敗"
fi

# -----------------------------------------------------------------------------
section "測試 10：清理"
if [[ "${KEEP_TOPIC}" == "true" ]]; then
  info "KEEP_TOPIC=true，保留 ${TOPIC}"
  ok "略過清理"
else
  if kafka_topics --delete --topic "${TOPIC}" >/dev/null 2>&1; then
    sleep 2
    if kafka_topics --list 2>/dev/null | grep -qx "${TOPIC}"; then
      info "topic 仍在刪除中（非同步刪除，屬正常）"
    fi
    ok "已刪除測試 topic"
    KEEP_TOPIC=true   # 避免 trap 重複刪除
  else
    ng "刪除 topic 失敗（delete.topic.enable=false？）"
  fi
fi

# -----------------------------------------------------------------------------
section "測試結果"
printf '  通過：%s%d%s   失敗：%s%d%s\n' "${C_GRN}" "${PASS}" "${C_RESET}" "${C_RED}" "${FAIL}" "${C_RESET}"
if (( FAIL > 0 )); then
  printf '\n  失敗項目：\n'
  for t in "${FAILED_TESTS[@]}"; do printf '    - %s\n' "$t"; done
  exit 1
fi
log_ok "冒煙測試全數通過"
