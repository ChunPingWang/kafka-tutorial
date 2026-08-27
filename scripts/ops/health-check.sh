#!/usr/bin/env bash
# =============================================================================
# health-check.sh - 叢集健康檢查（適合放進 cron / 監控系統）
#
# 檢查項目（共 10 項）：
#   1. Broker 存活數
#   2. KRaft controller quorum（leader 是否存在、follower 落後量）
#   3. Under-replicated partition
#   4. Under-min-ISR partition（acks=all 的寫入是否正被拒絕）
#   5. Unavailable partition（沒有 leader）
#   6. At-min-ISR partition（再掉一個副本就會停寫）
#   7. Topic 數量
#   8. Consumer group lag 超標
#   9. 本機磁碟使用率
#  10. Broker 之間的資料量分布
#
# 用法：
#   ./scripts/ops/health-check.sh                 # 人類可讀
#   ./scripts/ops/health-check.sh --format json   # 給監控系統
#   LAG_THRESHOLD=50000 ./scripts/ops/health-check.sh
#
# 退出碼：0 = 健康，1 = 警告，2 = 嚴重
# =============================================================================
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

FORMAT="text"
LAG_THRESHOLD="${LAG_THRESHOLD:-10000}"
DISK_WARN_PCT="${DISK_WARN_PCT:-80}"
DISK_CRIT_PCT="${DISK_CRIT_PCT:-90}"
EXPECTED_BROKERS="${EXPECTED_BROKERS:-0}"   # 0 = 不檢查

while [[ $# -gt 0 ]]; do
  case "$1" in
    --format) FORMAT="$2"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知選項：$1" ;;
  esac
done

SEVERITY=0   # 0 ok / 1 warn / 2 crit
declare -a RESULTS=()

record() {   # record <name> <status> <message>
  RESULTS+=("$1|$2|$3")
  case "$2" in
    CRIT) (( SEVERITY < 2 )) && SEVERITY=2 ;;
    WARN) (( SEVERITY < 1 )) && SEVERITY=1 ;;
  esac
  # 一定要 return 0：上面的 (( )) 在 SEVERITY 已達上限時會回傳 1，
  # 沒有這行的話 set -e 會在「第二個非 OK 檢查」時殺掉整個腳本
  return 0
}

emit_text() {
  section "叢集健康檢查 — ${BOOTSTRAP_SERVERS}"
  local r name st msg colour
  for r in "${RESULTS[@]}"; do
    IFS='|' read -r name st msg <<<"${r}"
    case "${st}" in
      OK)   colour="${C_GRN}" ;;
      WARN) colour="${C_YEL}" ;;
      CRIT) colour="${C_RED}" ;;
      *)    colour="${C_DIM}" ;;
    esac
    printf '  %s%-4s%s %-24s %s\n' "${colour}" "${st}" "${C_RESET}" "${name}" "${msg}"
  done
  printf '\n'
  case "${SEVERITY}" in
    0) printf '  %s整體狀態：健康%s\n' "${C_GRN}" "${C_RESET}" ;;
    1) printf '  %s整體狀態：警告%s\n' "${C_YEL}" "${C_RESET}" ;;
    2) printf '  %s整體狀態：嚴重%s\n' "${C_RED}" "${C_RESET}" ;;
  esac
}

emit_json() {
  local first=1 r name st msg
  printf '{"bootstrap":"%s","checked_at":"%s","severity":%d,"checks":[' \
    "${BOOTSTRAP_SERVERS}" "$(timestamp)" "${SEVERITY}"
  for r in "${RESULTS[@]}"; do
    IFS='|' read -r name st msg <<<"${r}"
    (( first )) || printf ','
    first=0
    printf '{"name":"%s","status":"%s","message":"%s"}' \
      "${name}" "${st}" "$(printf '%s' "${msg}" | sed 's/"/\\"/g')"
  done
  printf ']}\n'
}

# -----------------------------------------------------------------------------
# 1. Broker 存活
# -----------------------------------------------------------------------------
if ! cluster_ready; then
  record "broker_connectivity" CRIT "無法連線到 ${BOOTSTRAP_SERVERS}"
  [[ "${FORMAT}" == "json" ]] && emit_json || emit_text
  exit 2
fi

BROKERS="$("$(kafka_bin kafka-broker-api-versions.sh)" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    ${KAFKA_CLIENT_CONFIG:+--command-config "${KAFKA_CLIENT_CONFIG}"} 2>/dev/null \
    | grep -cE '^\S+:[0-9]+ \(id:' || true)"
if (( EXPECTED_BROKERS > 0 )) && (( BROKERS < EXPECTED_BROKERS )); then
  record "broker_count" CRIT "存活 ${BROKERS} 個，預期 ${EXPECTED_BROKERS} 個"
else
  record "broker_count" OK "存活 ${BROKERS} 個 broker"
fi

# -----------------------------------------------------------------------------
# 2. KRaft quorum
# -----------------------------------------------------------------------------
if QUORUM="$(kafka_metadata describe --status 2>/dev/null)"; then
  LEADER_ID="$(awk -F: '/LeaderId/{gsub(/ /,"",$2); print $2}' <<<"${QUORUM}")"
  MAX_LAG="$(awk -F: '/MaxFollowerLag:/{gsub(/ /,"",$2); print $2}' <<<"${QUORUM}")"
  if [[ -z "${LEADER_ID}" || "${LEADER_ID}" == "-1" ]]; then
    record "kraft_quorum" CRIT "quorum 沒有 leader，metadata 無法更新"
  elif [[ -n "${MAX_LAG}" ]] && (( MAX_LAG > 1000 )); then
    record "kraft_quorum" WARN "leader=${LEADER_ID}，但 follower 落後 ${MAX_LAG} 筆"
  else
    record "kraft_quorum" OK "leader=${LEADER_ID}，follower lag=${MAX_LAG:-0}"
  fi
else
  record "kraft_quorum" WARN "無法查詢 quorum 狀態"
fi

# -----------------------------------------------------------------------------
# 3~5. Partition 健康
# -----------------------------------------------------------------------------
UR="$(kafka_topics --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:' || true)"
if (( UR > 0 )); then
  record "under_replicated" CRIT "${UR} 個 partition 副本不足（有 broker 掛掉或同步落後）"
else
  record "under_replicated" OK "0"
fi

UMISR="$(kafka_topics --describe --under-min-isr-partitions 2>/dev/null | grep -c 'Topic:' || true)"
if (( UMISR > 0 )); then
  record "under_min_isr" CRIT "${UMISR} 個 partition 低於 min.insync.replicas，acks=all 的寫入已被拒絕"
else
  record "under_min_isr" OK "0"
fi

UNAVAIL="$(kafka_topics --describe --unavailable-partitions 2>/dev/null | grep -c 'Topic:' || true)"
if (( UNAVAIL > 0 )); then
  record "unavailable" CRIT "${UNAVAIL} 個 partition 沒有 leader，讀寫都會失敗"
else
  record "unavailable" OK "0"
fi

AT_MIN="$(kafka_topics --describe --at-min-isr-partitions 2>/dev/null | grep -c 'Topic:' || true)"
if (( AT_MIN > 0 )) && (( BROKERS > 1 )); then
  record "at_min_isr" WARN "${AT_MIN} 個 partition 剛好等於 min.insync.replicas，再掉一個副本就會停寫"
elif (( AT_MIN > 0 )); then
  # 單機叢集 RF=1、min.insync.replicas=1，本來就「剛好等於」，不算異常
  record "at_min_isr" OK "${AT_MIN}（單機叢集，屬預期）"
else
  record "at_min_isr" OK "0"
fi

TOPIC_COUNT="$(kafka_topics --list 2>/dev/null | grep -vc '^__' || true)"
record "topic_count" OK "${TOPIC_COUNT} 個使用者 topic"

# -----------------------------------------------------------------------------
# 6. Consumer group lag
# -----------------------------------------------------------------------------
CG_LIST="$(kafka_groups --list 2>/dev/null | grep -v '^$' || true)"
CG_COUNT="$(printf '%s\n' "${CG_LIST}" | grep -c . || true)"
HIGH_LAG_GROUPS=""
if (( CG_COUNT > 0 )); then
  while IFS= read -r g; do
    [[ -z "${g}" ]] && continue
    local_lag="$(kafka_groups --describe --group "${g}" 2>/dev/null \
      | awk '$6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}' || true)"
    [[ -n "${local_lag}" ]] || local_lag=0   # group 剛被刪除等暫時性失敗不該中斷整個檢查
    if (( local_lag > LAG_THRESHOLD )); then
      HIGH_LAG_GROUPS="${HIGH_LAG_GROUPS} ${g}(${local_lag})"
    fi
  done <<<"${CG_LIST}"
fi
if [[ -n "${HIGH_LAG_GROUPS}" ]]; then
  record "consumer_lag" WARN "超過門檻 ${LAG_THRESHOLD} 的 group：${HIGH_LAG_GROUPS# }"
else
  record "consumer_lag" OK "${CG_COUNT} 個 group，皆低於 ${LAG_THRESHOLD}"
fi

# -----------------------------------------------------------------------------
# 7. 本機磁碟
# -----------------------------------------------------------------------------
if [[ -d "${KAFKA_DATA_DIR}" ]]; then
  USED_PCT="$(df -P "${KAFKA_DATA_DIR}" | awk 'NR==2{gsub(/%/,"",$5); print $5}')"
  USED_H="$(df -Ph "${KAFKA_DATA_DIR}" | awk 'NR==2{print $3" / "$2}')"
  if (( USED_PCT >= DISK_CRIT_PCT )); then
    record "disk_usage" CRIT "${KAFKA_DATA_DIR} 已用 ${USED_PCT}%（${USED_H}）"
  elif (( USED_PCT >= DISK_WARN_PCT )); then
    record "disk_usage" WARN "${KAFKA_DATA_DIR} 已用 ${USED_PCT}%（${USED_H}）"
  else
    record "disk_usage" OK "${KAFKA_DATA_DIR} 已用 ${USED_PCT}%（${USED_H}）"
  fi
fi

# -----------------------------------------------------------------------------
# 8. Broker 之間的資料量分布
# -----------------------------------------------------------------------------
if command -v jq >/dev/null 2>&1; then
  LOGDIRS_JSON="$(kafka_logdirs --describe 2>/dev/null | grep -E '^\{' | tail -1 || true)"
  if [[ -n "${LOGDIRS_JSON}" ]]; then
    SIZES="$(printf '%s' "${LOGDIRS_JSON}" | jq -r '
      .brokers[] | "\(.broker) \([.logDirs[].partitions[].size] | add // 0)"' 2>/dev/null || true)"
    if [[ -n "${SIZES}" ]]; then
      SUMMARY="$(while read -r b sz; do
        printf 'broker%s=%s ' "${b}" "$(human_bytes "${sz}")"
      done <<<"${SIZES}")"
      # 判斷失衡：最大 / 最小 > 2 且最小 > 1GB
      MAXV="$(awk '{print $2}' <<<"${SIZES}" | sort -n | tail -1)"
      MINV="$(awk '{print $2}' <<<"${SIZES}" | sort -n | head -1)"
      if (( MINV > 1073741824 )) && (( MAXV > MINV * 2 )); then
        record "data_balance" WARN "broker 間資料量失衡：${SUMMARY}"
      else
        record "data_balance" OK "${SUMMARY}"
      fi
    fi
  fi
else
  record "data_balance" OK "略過（未安裝 jq）"
fi

# -----------------------------------------------------------------------------
if [[ "${FORMAT}" == "json" ]]; then emit_json; else emit_text; fi
exit "${SEVERITY}"
